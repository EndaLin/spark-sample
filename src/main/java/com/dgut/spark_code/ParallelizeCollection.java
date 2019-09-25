package com.dgut.spark_code;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ParallelizeCollection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> integerJavaRDD = sparkContext.parallelize(numbers);

        // 执行reduce 算子操作
        // 将第一个和第二个的执行结果，与第三个进行运算，以此类推
        int sum = integerJavaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println("answers: " + sum);

        // 关闭
        sparkContext.close();
    }
}
