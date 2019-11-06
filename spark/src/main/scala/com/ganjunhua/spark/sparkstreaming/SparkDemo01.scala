package com.ganjunhua.spark.sparkstreaming

import java.io.File
import java.nio.file.FileSystem

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDemo01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(10))

    val localFile = ssc.textFileStream("D:" + File.separator + "spark")

    val words = localFile.flatMap(x => x.split(","))
    val wordsPairs = words.map(x => (x, 1)).filter(x => x._1 != "java")
    val wordCount = wordsPairs.reduceByKey(_ + _)
    wordCount.count().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
