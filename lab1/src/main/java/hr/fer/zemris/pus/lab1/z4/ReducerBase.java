package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;

import java.io.IOException;

public interface ReducerBase<K1, V1, K2, V2> extends Reducer<K1, V1, K2, V2> {
    @Override
    public default void close() throws IOException {

    }

    @Override
    public default void configure(JobConf jobConf) {

    }
}
