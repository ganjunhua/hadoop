package com.ganjunhua.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CountHtml {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("data/access.log")
    val count404 = sc.longAccumulator("404")
    val count200 = sc.longAccumulator("200")
    val countSum = sc.longAccumulator("sum")
    dataRDD.foreach(x => {
      val lines = x.split(",")
      val word = lines(0)
      if (word == "404") {
        count404.add(1)
      }
      if (word == "200") {
        count200.add(1)
      }
      countSum.add(1)
    })
    println(" count404 = " + count404.value)
    println(" count200 = " + count200.value)
    println(" countSum = " + countSum.value)
    sc.stop()
  }
}
