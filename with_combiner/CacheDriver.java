package Task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CacheDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: CacheDriver <inputDir> <outputDir> <cacheFile>");
            return 1;
        }

        String inputDir = args[0];
        String outputDir = args[1];
        String cachePath = args[2];
        Configuration conf = getConf();

        conf.set("cache.file.path", cachePath);

        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(new Path(cachePath))) {
            System.err.println("ERROR: Cache file not found on HDFS: " + cachePath);
            return 1;
        }
        Path output = new Path(outputDir);
        if (fs.exists(output)) {
            fs.delete(output, true);
            System.out.println("Deleted existing output directory: " + outputDir);
        }

        Job job = Job.getInstance(conf, "Task 3 Web Log URL Categorization");

        job.setJarByClass(CacheDriver.class);

        job.setMapperClass(CacheMapper.class);
        job.setReducerClass(CacheReducer.class);
        job.setCombinerClass(CacheCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, output);
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new CacheDriver(), args);
        System.exit(exitCode);
    }
}


        

        