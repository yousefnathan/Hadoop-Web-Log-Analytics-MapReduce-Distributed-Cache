package Task3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * CacheMapper
 *
 * Input  : web_logs_1gb.txt       -> requestId, urlPath, responseTimeMs, statusCode
 * Cache  : url_categories.txt     -> urlPattern, category
 * Output : category, responseTimeMs|statusCode
 */
public class CacheMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(CacheMapper.class);

    private final Map<String, String> categoryMap = new HashMap<String, String>();
    private final Text outputKey = new Text();
    private final Text outputVal = new Text();

    private int skippedRecords = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String cachePath = context.getConfiguration().get("cache.file.path");

        if (cachePath == null || cachePath.isEmpty()) {
            throw new IOException("Configuration key 'cache.file.path' is not set.");
        }

        LOG.info("Reading cache file from HDFS: " + cachePath);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path hdfsPath = new Path(cachePath);

        BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(hdfsPath)));

        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();

            if (line.isEmpty()) {
                continue;
            }

            String[] parts = line.split(",", 2);

            if (parts.length == 2) {
                String urlPattern = parts[0].trim();
                String category = parts[1].trim();

                categoryMap.put(urlPattern, category);
            }
        }
        br.close();

        LOG.info("Cache loaded - " + categoryMap.size() + " URL categories.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty()) {
            skippedRecords++;
            return;
        }

        String[] parts = line.split(",", 4);

        if (parts.length < 4) {
            skippedRecords++;
            LOG.warn("Malformed line skipped: " + line);
            return;
        }

        String urlPath = parts[1].trim();

        int responseTime;
        int statusCode;
        try {
            responseTime = Integer.parseInt(parts[2].trim());
            statusCode = Integer.parseInt(parts[3].trim());
        } catch (NumberFormatException e) {
            skippedRecords++;
            LOG.warn("Bad numeric value in line: " + line);
            return;
        }

        String category = "OTHER";
        for (String pattern : categoryMap.keySet()) {
            if (urlPath.startsWith(pattern)) {
                category = categoryMap.get(pattern);
                break;
            }
        }
        outputKey.set(category);
        int error = 0;
        if (statusCode >= 400) {
            error = 1;
        }

        outputVal.set("1|" + responseTime + "|" + error);

        context.write(outputKey, outputVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Skipped Records: " + skippedRecords);
        categoryMap.clear();
        super.cleanup(context);
    }
}


        

        
        