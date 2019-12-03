package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Red1 extends MapReduceBase implements Reducer<IntWritable, IntArrayWriteable, IntArrayWriteable, IntWritable> {

    @Override
    public void reduce(IntWritable intWritable, Iterator<IntArrayWriteable> arrayWritableIterator, OutputCollector<IntArrayWriteable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        List<IntWritable[]> a = new LinkedList<>();
        List<IntWritable[]> b = new LinkedList<>();
        arrayWritableIterator.forEachRemaining(
                arrayWritable -> {
                    Writable[] w = arrayWritable.get();
                    if (((IntWritable) w[0]).get() == 0) {
                        a.add(new IntWritable[]{((IntWritable) w[0]), ((IntWritable) w[1]), ((IntWritable) w[2])});
                    } else {
                        b.add(new IntWritable[]{((IntWritable) w[0]), ((IntWritable) w[1]), ((IntWritable) w[2])});
                    }
                }
        );
        for (IntWritable[] as : a) {
            for (IntWritable[] bs : b) {
                IntWritable mul = new IntWritable(as[2].get() * bs[2].get());
                outputCollector.collect(new IntArrayWriteable(as[1], bs[1]), mul);
            }
        }
    }
}