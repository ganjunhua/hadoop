package com.ganjunhua.spark.exercises

import org.apache.spark.{SparkConf, SparkContext}

object SparkCore {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("SparkCore")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val path = "hdfs://192.168.182.141:8020/tmp/test.txt"
    val rows = sc.textFile(path)
    val rowRDD = rows.flatMap(line => line.split("\\^")).map(x => (x, 1)).filter(x => x._1 != "hive")
      //.reduceByKey((x, y) => (x + y))
      .reduceByKey(_+_,10)
    //rowRDD.repartition(1).saveAsTextFile("hdfs://192.168.182.141:8020/tmp/tes4.txt")
    rowRDD.foreach(print)
    sc.stop()
  }
}
