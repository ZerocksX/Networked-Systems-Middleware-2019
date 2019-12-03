package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, IntArrayWriteable, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntArrayWriteable, IntWritable> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("\\s+");
        IntArrayWriteable ij = new IntArrayWriteable(new IntWritable(Integer.valueOf(parts[0])), new IntWritable(Integer.valueOf(parts[1])));
        IntWritable m = new IntWritable(Integer.valueOf(parts[2]));
        output.collect(ij, m);
    }
}
