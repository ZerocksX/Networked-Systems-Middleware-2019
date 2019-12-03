package hr.fer.zemris.pus.lab1.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class VideoCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int THRESHOLD = 5;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("[ ]");
        String id = parts[0];
        int percent = Integer.parseInt(parts[1]);
        if (percent > THRESHOLD) {
            output.collect(new Text(id), new IntWritable(percent));
        }
    }
}
