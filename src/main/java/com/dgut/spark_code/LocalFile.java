package com.dgut.spark_code;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LocalFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 针对本地文件创建RDD
        JavaRDD<String> lines = sparkContext.textFile("./word.txt");

        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.split(" ").length;
            }
        });

        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println(count);

        sparkContext.close();
    }
}
