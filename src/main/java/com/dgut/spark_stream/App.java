package com.dgut.spark_stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

/**
 * Spark sample
 */
public class App {

    private static final String PATH = "/home/wt/桌面/20190805/20190805_action_10.csv";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStream").setMaster("local[2]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(1000));

        JavaDStream<String> lines = streamingContext.textFileStream(PATH);

//        JavaDStream<String> words = lines.flatMap(
//                new FlatMapFunction<String, String>() {
//                    public Iterable<String> call(String x) {
//                        return Arrays.asList(x.split(","));
//                    }
//                });
    }
}
