package hr.fer.zemris.pus.lab1.z4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MatMul {


    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.err.println("Usage: MatMul <input path>");
            System.exit(-1);
        }

        long millis = System.currentTimeMillis();

        Path input = new Path(args[0]);
        Path temp = new Path("output-" + millis + "/temp");
        Path output = new Path("output-" + millis + "/final");


        JobConf jobConf1 = new JobConf(MatMul.class);
        jobConf1.setJobName("Matrix multiplication 1");

        jobConf1.setMapperClass(Map1.class);
        jobConf1.setReducerClass(Red1.class);
        jobConf1.setOutputKeyClass(IntWritable.class);
        jobConf1.setOutputValueClass(IntArrayWriteable.class);


        FileInputFormat.addInputPath(jobConf1, input);
        FileOutputFormat.setOutputPath(jobConf1, temp);

        JobConf jobConf2 = new JobConf(MatMul.class);
        jobConf2.setJobName("Matrix multiplication 2");

        jobConf2.setMapperClass(Map2.class);
        jobConf2.setReducerClass(Red2.class);
        jobConf2.setOutputKeyClass(IntArrayWriteable.class);
        jobConf2.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(jobConf2, temp);
        FileOutputFormat.setOutputPath(jobConf2, output);

        JobClient.runJob(jobConf1);
        JobClient.runJob(jobConf2);
    }
}
