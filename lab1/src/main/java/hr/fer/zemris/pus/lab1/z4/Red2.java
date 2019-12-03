package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class Red2 extends MapReduceBase implements Reducer<IntArrayWriteable, IntWritable, IntArrayWriteable, IntWritable> {

    @Override
    public void reduce(IntArrayWriteable key, Iterator<IntWritable> values, OutputCollector<IntArrayWriteable, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()){
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }
}
