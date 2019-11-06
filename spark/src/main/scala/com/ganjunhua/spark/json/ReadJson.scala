package com.ganjunhua.spark.json

import java.io.FileInputStream

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession


object ReadJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //解析 结果化json
    val path = "data/json"
    val jsonRDD1 = spark.sparkContext
      .textFile("data/json/test.json")
    val dataRDD1 = jsonRDD1.map(json => {
      val jsonObject = JSON.parseObject(json)
      val name = jsonObject.getOrDefault("name", null)
      val age = jsonObject.getOrDefault("age", null)
      (name, age)
    })
    dataRDD1.foreach(println)
  //  dataRDD1.repartition(1).saveAsTextFile("data/json/test")
    spark.stop()
  }
}