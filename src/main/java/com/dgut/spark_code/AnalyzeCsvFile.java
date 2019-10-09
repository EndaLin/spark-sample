package com.dgut.spark_code;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class AnalyzeCsvFile {

    private static final String PATH = "/home/wt/桌面/20190805/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("AnalyzeCsvFile")
                .master("local[2]")
                .getOrCreate();

        StructType dataSchema = new StructType()
                .add("number", DataTypes.StringType, true)
                .add("unknown_1", DataTypes.StringType, true)
                .add("url", DataTypes.StringType, true)
                .add("id", DataTypes.StringType, true)
                .add("src_ip", DataTypes.StringType, true)
                .add("src_ip_hex", DataTypes.StringType, true)
                .add("dst_ip", DataTypes.StringType, true)
                .add("dst_ip_hex", DataTypes.StringType, true)
                .add("protocol", DataTypes.StringType, true)
                .add("unknown_2", DataTypes.StringType, true)
                .add("terminal_type", DataTypes.StringType, true)
                .add("access_type", DataTypes.StringType, true)
                .add("access_site", DataTypes.StringType, true)
                .add("client_port", DataTypes.StringType, true)
                .add("service_port", DataTypes.StringType, true)
                .add("unknown_3", DataTypes.StringType, true)
                .add("access_time", DataTypes.StringType, true)
                .add("xml", DataTypes.StringType, true);

        Dataset<Row> df = spark
                .readStream()
                .option("multiLine", true)
                .option("escape", "\"")
                .schema(dataSchema)
                .csv(PATH);

        spark.udf().register("xmlToJson", new XmlToJson(), DataTypes.StringType);
        // xml to json
        Dataset<Row> df2 = df.selectExpr("number", "xmlToJson(xml) as json");

        Dataset<Row> df3 = df2.join(df.drop("xml"), "number");

        try {
            StreamingQuery query = df3.writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();
            query.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
