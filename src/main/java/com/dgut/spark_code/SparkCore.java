//package com.dgut.spark_code;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.sql.sources.In;
//import org.codehaus.janino.Java;
//import org.junit.Before;
//import org.junit.Test;
//import scala.Int;
//import scala.Tuple2;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//
///**
// * 算子练习
// */
//public class SparkCore implements Serializable {
//
//    private transient static SparkConf spark;
//
//    private transient static JavaSparkContext context;
//
//    @Before
//    public void init() {
//        spark = new SparkConf()
//                .setAppName("SparkCore")
//                .setMaster("local[2]");
//
//        context = new JavaSparkContext(spark);
//    }
//
//
//    @Test
//    public void map() {
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source = context.parallelize(data);
//
//        JavaRDD<Integer> result = source.map(s -> s * 2);
//
//        System.out.println(result.collect());
//    }
//
//    @Test
//    public void reduce() {
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source = context.parallelize(data);
//
//        int result = source.reduce(((a, b) -> a + b));
//        System.out.println(result);
//    }
//
//    @Test
//    public void filter() {
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source = context.parallelize(data);
//
//        JavaRDD<Integer> result = source.filter(s -> s % 2 == 0);
//        System.out.println(result.collect());
//    }
//
//    @Test
//    public void flatMap() {
//        List<String> data = Arrays.asList("hello world", "java spark");
//        JavaRDD<String> source = context.parallelize(data);
//
//        JavaRDD<String> result = source.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        System.out.println(result.collect());
//    }
//
//
//    /**
//     * 对每个分区进行一次Function
//     */
//    @Test
//    public void mapPartition() {
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source = context.parallelize(data, 2);
//
//        JavaRDD<Integer> result = source.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
//            @Override
//            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
//                int sum = 0;
//                while (integerIterator.hasNext()) {
//                    sum += integerIterator.next();
//                }
//                return Arrays.asList(sum).iterator();
//            }
//        });
//
//        System.out.println(result.collect());
//    }
//
//    /**
//     * sample(withReplacement, fraction, seed)
//     * 以指定的随机种子随机抽样出数量为fraction的数据，withReplacement 表示是否抽出的数据要放回，Seed为指定随机数生成器种子
//     */
//    @Test
//    public void sample() {
//        List<Integer> data = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source = context.parallelize(data);
//
//        JavaRDD<Integer> result = source.sample(true, 0.4, 2);
//        System.out.println(result.collect());
//    }
//
//
//    /**
//     * 合并操作
//     */
//    @Test
//    public void union() {
//        List<Integer> data1 = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source1 = context.parallelize(data1);
//
//        List<Integer> data2 = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
//        JavaRDD<Integer> source2 = context.parallelize(data2);
//
//        JavaRDD<Integer> result = source1.union(source2);
//        System.out.println(result.collect());
//    }
//
//    /**
//     * 去重操作
//     */
//    @Test
//    public void distinct() {
//        List<Integer> data = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9);
//        JavaRDD<Integer> source = context.parallelize(data);
//
//        JavaRDD<Integer> result = source.distinct();
//        System.out.println(result.sortBy(x -> x, false, 1).collect());
//    }
//
//    @Test
//    public void reduceByKey() {
//        JavaRDD<String> source = context.textFile("./word.txt");
//
//        JavaPairRDD<String, Integer> words = source.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> count = words.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//
//        count.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                System.out.println(tuple._1 + " " + tuple._2);
//            }
//        });
//    }
//
//    @Test
//    public void groupByKey() {
//        JavaRDD<String> source = context.textFile("./word.txt");
//
//        JavaPairRDD<String, Integer> words = source.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<>(s, 1);
//            }
//        });
//
//        JavaPairRDD<String, Iterable<Integer>> result = words.groupByKey();
//        result.foreach(data -> System.out.println(data));
//    }
//
//    @Test
//    public void join() {
//
//    }
//}
