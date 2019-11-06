package com.ganjunhua.spark.json

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession

object ReadUnstructuredJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val jsonData = spark
      .sparkContext
      .textFile("data/json/unstructJson.json")

    val dataRDD = jsonData.flatMap(json => {
      //将每行数据打平，生成一个数组
      val jsonObject = JSON.parseObject(json).getJSONArray("data")
      // 创建数组容器
      var dataList: List[String] = List()
      for (i <- 0 to jsonObject.size() - 1) {
        // 将数组添加至dataList
        dataList = jsonObject.get(i).toString :: dataList
      }
      dataList
    }).map(x => JSON.parseObject(x)) // 解析每一行json数据

    //通过key获取值
    val jsonObject1 = dataRDD.map(json => {
      val acc = json.getOrDefault("acc", null)
      val label = json.getOrDefault("label", null)
      val version = json.getOrDefault("version", null)
      (acc, label, version)
    }).map(x => x._1)
    jsonObject1.foreach(println)

    spark.stop()
  }
}
