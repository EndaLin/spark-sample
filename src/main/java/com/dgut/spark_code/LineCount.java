package com.dgut.spark_code;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LineCount")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建初始RDD
        JavaRDD<String> lines = sc.textFile("./word.txt");

        JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

       flatMapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
