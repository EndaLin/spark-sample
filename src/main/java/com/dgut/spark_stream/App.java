package com.dgut.spark_stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Spark sample
 */
public class App {

    private static final String PATH = "./file";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkStream")
                .setMaster("local[2]");

        // 第二个参数表面，没收集多次的数据，划分为一个batch,这里是1 秒
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 创建输入DStream，代表了一个从数据源持续不断的实时数据流
        JavaDStream<String> lines = streamingContext.textFileStream(PATH);

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println("output: " + s);
                        System.out.println("*********************");
                    }
                });
            }
        });
        

        // 调用JavaStreamingContext 的start 方法，整个Spark Streaming Application才会执行
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
