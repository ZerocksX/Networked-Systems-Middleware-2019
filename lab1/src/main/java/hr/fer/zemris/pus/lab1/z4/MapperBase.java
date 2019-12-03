package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

public interface MapperBase<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {


    @Override
    public default void close() throws IOException {

    }

    @Override
    public default void configure(JobConf jobConf) {

    }
}
