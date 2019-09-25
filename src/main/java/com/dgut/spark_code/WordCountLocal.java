package com.dgut.spark_code;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountLocal {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local[2]");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        // 创建一个RDD，输入源的数据会被打散到RDD 的各个分区中
        // 在JAVA 中创建的RDD 都是JavaRDD
        // 在SparkContext 中，根据文件类型的输入源创建RDD 的方法叫textFile() 方法
        JavaRDD<String> lines = context.textFile("./word.txt");

        // 计算
        // 一般计算操作都会通过创建function，并配合RDD的map、flatMap 等算子来执行
        // function 比较简单的话，会创建匿名内部类来执行，如果比较复杂的话，则会单独创建一个类来执行

        // 将每一行拆分中一个个单词
        // FlatMapFunction 有两个泛型参数，分别代表输入和输出类型
        // 此处输入的是String ，代表一行行文本，输出也是String
        // flatMap 算子作用是将RDD 一个元素拆分成一个或者多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        // tuple 类型？
        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                }
        );

        // 以单词为key，统计每个单词出现的次数
        // JavaPairRDD 的元素相当于(hello,1)、(hello,1)、(hello,1)、(hello,1)
        // reduce 操作相当于把第一个值和第二个值进行计算，然后把结果和第三个值进行计算
        final JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );

        // 到这里位置，我们通过几个Spark  算子操作，已经统计出单词的次数
        // 但是之前我们只用的算子操作都是transformation 操作
        // 一个Spark应用中单纯用有transformation 是不会执行的，还需要有action 来触发程序执行
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }
        });

        context.close();
    }
}
