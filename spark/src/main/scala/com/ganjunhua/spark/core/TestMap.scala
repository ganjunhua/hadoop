package com.ganjunhua.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object TestMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("xx")
    val sc = new SparkContext(conf)
    val stopWordData = sc.textFile("data/textfile/stopword.txt")
    // var data= scala.collection.mutable.Map[String,Int]()
    var data = ArrayBuffer[String]()
    val map2 = stopWordData.map(
      x => {
        data.append(x)
        data
      }
    ).collect().flatten.toList
    /*
    stopWordData.collect().foreach(x => {
      data += (x.toString -> 1)

    })
    for (i <- 0 to 9) {

    }*/
    //  map2.foreach(x=>(print(x._2)))
    for (i <- 0 to map2.length) {
      if (map2(i).equals("ganjunhua")){
        println("===========================")
        println(map2(i))
      }
    }
    sc.stop()
  }
}
