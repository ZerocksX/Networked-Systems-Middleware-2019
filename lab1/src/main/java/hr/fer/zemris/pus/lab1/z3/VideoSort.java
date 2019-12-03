package hr.fer.zemris.pus.lab1.z3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class VideoSort {

    public static final IntWritable one = new IntWritable(1);

    public static class MapCount extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String[] parts = value.toString().split("\\s+");
            String id = parts[0];
            output.collect(new Text(id), one);
        }
    }

    public static class RedSum extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static class MapInvert extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] parts = value.toString().split("\\s+");
            output.collect(new IntWritable(-1 * Integer.valueOf(parts[1])), new Text(parts[0]));
        }
    }

    public static class RedPrint extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(values.next(), new IntWritable(key.get() * -1));
            }
        }
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.err.println("Usage: VideoSort <input path>");
            System.exit(-1);
        }

        long millis = System.currentTimeMillis();

        Path input = new Path(args[0]);
        Path temp = new Path("output-" + millis + "/temp");
        Path output = new Path("output-" + millis + "/final");


        JobConf jobConf1 = new JobConf(VideoSort.class);
        jobConf1.setJobName("Matrix multiplication 1");

        jobConf1.setMapperClass(MapCount.class);
        jobConf1.setReducerClass(RedSum.class);
        jobConf1.setOutputKeyClass(Text.class);
        jobConf1.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(jobConf1, input);
        FileOutputFormat.setOutputPath(jobConf1, temp);

        JobConf jobConf2 = new JobConf(VideoSort.class);
        jobConf2.setJobName("Matrix multiplication 2");

        jobConf2.setMapperClass(MapInvert.class);
        jobConf2.setReducerClass(RedPrint.class);
        jobConf2.setOutputKeyClass(IntWritable.class);
        jobConf2.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(jobConf2, temp);
        FileOutputFormat.setOutputPath(jobConf2, output);

        JobClient.runJob(jobConf1);
        JobClient.runJob(jobConf2);
    }
}
