package com.ganjunhua.spark.sparksql

import org.apache.spark.sql.SparkSession

object SparkJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    val jsonData = spark.read.format("json").load("data/json/unstructJson.json")
    jsonData.printSchema()

    spark.stop()
  }
}
