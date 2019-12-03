package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntArrayWriteable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntArrayWriteable> outputCollector, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("[ ]");
        IntWritable r = new IntWritable(Integer.valueOf(parts[1]));
        IntWritable c = new IntWritable(Integer.valueOf(parts[2]));
        IntWritable v = new IntWritable(Integer.valueOf(parts[3]));
        if (parts[0].equals("a")) {
            IntWritable M = new IntWritable(0);
            outputCollector.collect(c, new IntArrayWriteable(M, r, v));
        } else {
            IntWritable M = new IntWritable(1);
            outputCollector.collect(r, new IntArrayWriteable(M, c, v));
        }
    }
}