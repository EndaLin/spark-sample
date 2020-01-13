package com.dgut.spark_core

import org.apache.spark.sql.SparkSession

/**
 * @author linwt
 * @date 2019/11/27 14:51
 */
object SparkSample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Sample")
      .config("spark.sql.warehouse.dir", "F:\\IDEA\\spark-sample\\src\\main\\resources")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("select * from test_log_orc")

//    val content = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:hive2://centos-l1.niracler.com:10000")
//      .option("dbtable", "test_log_orc")
//      .option("driver", "org.apache.hive.jdbc.HiveDriver")
//      .load()

    df.show()

  }
}
