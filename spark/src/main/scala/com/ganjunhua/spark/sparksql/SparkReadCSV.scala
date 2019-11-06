package com.ganjunhua.spark.sparksql

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkReadCSV {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://holiday-1:9083")
      .enableHiveSupport()
      .getOrCreate()
    val clickCSV = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/jd/t_click.csv")
    clickCSV.printSchema()
    clickCSV.select("*")
      .write.mode(SaveMode.Append).saveAsTable("default.holiday_click")
    // .write.saveAsTable("default.holiday20180819")

    spark.stop()
  }
}
