package com.dgut.spark_code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class AnalyzeCsvFile {

    private static final String PATH = "/home/wt/桌面/20190805/20190805_action_10.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("AnalyzeCsvFile")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("multiLine", true)
                .option("escape", "\"")
                .csv(PATH);

        String[] columns = df.columns();

        spark.udf().register("xmlToJson", new XmlToJson(), DataTypes.StringType);

        // xml to json
        df.selectExpr("*", String.format("xmlToJson(%s)", columns[columns.length - 1])).show();
    }
}
