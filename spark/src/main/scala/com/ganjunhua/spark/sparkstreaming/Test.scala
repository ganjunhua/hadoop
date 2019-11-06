package com.ganjunhua.spark.sparkstreaming

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val list = List(("beo", 50), ("ain", 80), ("cin", 70))
    val listrdd = sc.parallelize(list, 1)
    val list1 = listrdd.sortByKey(true)
    val a = list1.countByKey()
    println(a)
    sc.stop()

  }
}
