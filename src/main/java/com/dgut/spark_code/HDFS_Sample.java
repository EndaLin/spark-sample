package com.dgut.spark_code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * @author linwt
 * @date 2019/11/15 13:05
 */
public class HDFS_Sample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark HDFS Example")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> rdd = spark.read().text("hdfs://centos-l1.niracler.com:9000/test/hello.txt");
        rdd.show();
    }
}
