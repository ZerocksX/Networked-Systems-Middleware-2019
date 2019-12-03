package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Arrays;

public class IntArrayWriteable extends ArrayWritable implements WritableComparable<IntArrayWriteable> {
    public IntArrayWriteable() {
        super(IntWritable.class);
    }

    public IntArrayWriteable(IntWritable... intWritables) {
        super(IntWritable.class);
        this.set(intWritables);
    }

    @Override
    public String[] toStrings() {
        return Arrays.stream(get()).map(writable -> (IntWritable) writable).map(String::valueOf).toArray(String[]::new);
    }

    @Override
    public String toString() {
        return String.join(" ", toStrings());
    }

    @Override
    public int compareTo(IntArrayWriteable o) {
        return this.toString().compareTo(o.toString());
    }
}
