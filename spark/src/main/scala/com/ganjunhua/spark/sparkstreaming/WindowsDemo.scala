package com.ganjunhua.spark.sparkstreaming

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.textFileStream("D:" + File.separator + "spark")
    val lineData = lines.window(Seconds(10), Seconds(10))
    lineData.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
