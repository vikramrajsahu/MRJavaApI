/**
 * Total Sum of Stock Volume
 * Sample Data
 * NYSE,MOTILAL,5000
 * BSE,FRANKLIN,3000
 * NYSE,ADITYA,6000
 * NIFTY,MOTILAL,2500
 * BSE,ADITYA,500
 * NYSE,FRANKLIN,300
 *
 * Hadoop CLI Command: hadoop jar MRJavaApI.jar stockMR/stocks stockMR/output
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MRDriver extends Configured implements Tool {
    public static class MRMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            System.out.println(line);
            System.out.println(parts.length);
            System.out.println(parts[1]);
            System.out.println(parts[2]);
            if (parts.length == 3) {
                String stock = parts[1];
                long volume = Long.parseLong(parts[2]);
                context.write(new Text(stock), new LongWritable(volume));
            }
        }
    }

    public static class MRReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new MRDriver(), args);
        System.exit(returnStatus);
    }

    public int run(String[] args) throws Exception {
        System.out.println("Configuration Running ....");
        // Configuration conf = new Configuration();
        Job job = new Job(getConf(), "Stock Aggregation Map Reduce Program");

        String dir = "hdfs://nameservice1/user/edureka_788309/";

        // Set Job Classes
        job.setMapperClass(MRMapper.class);
        job.setReducerClass(MRReducer.class);
        job.setJarByClass(MRDriver.class);

        // Map Output Key and Value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reducer Output Key and Value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Iterable.class);

        // Input Formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input and output Paths
        FileInputFormat.addInputPath(job, new Path(dir.concat(args[0])));
        FileOutputFormat.setOutputPath(job, new Path(dir.concat(args[1])));

        System.out.println("Started Running ....");
        return job.waitForCompletion(true) ? 1 : 0;
    }
}
