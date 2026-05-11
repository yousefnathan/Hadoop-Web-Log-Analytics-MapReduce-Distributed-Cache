package Task3;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CacheReducer extends Reducer<Text, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(CacheReducer.class);
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long requestCount = 0;
        long sumResponseTime = 0;
        long errorCount = 0;
        for (Text val : values) {
            String[] parts = val.toString().split("\\|");

            if (parts.length != 3) {
                continue;
            }

            try {
                requestCount += Long.parseLong(parts[0].trim());
                sumResponseTime += Long.parseLong(parts[1].trim());
                errorCount += Long.parseLong(parts[2].trim());
            } catch (NumberFormatException e) {
                LOG.warn("Bad reducer value skipped: " + val.toString());
            }
        }

        if (requestCount > 0) {
            long avgResponseTime = sumResponseTime / requestCount;
            result.set("Requests: " + requestCount
                    + "\tAvgResponseTime: " + avgResponseTime
                    + "\tErrors: " + errorCount);

            context.write(key, result);
        }
    }
}



        