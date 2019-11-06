package com.ganjunhua.spark.kafka

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object ReadKafka {
  def main(args: Array[String]): Unit = {
    var master = "local[*]"
    var appName = this.getClass.getSimpleName
    var second = 20

    if (args.length == 3) {
      master = args(0)
      appName = args(1)
      second = args(2).toInt
    }

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(second))
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "holiday-s:2181",
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    //val topic =  Map("test1" -> 1)
    val topic = Set("test1")
    //val kafkaData = KafkaUtils.createStream(ssc, "holiday-s:2181", "test", topic)
    val kafkaData = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)

    /* val events = kafkaData.flatMap(line => {
       println(line)
       println(line.getClass)
       val data = JSON.parseObject(line._2)
       println(data.getClass)
       Some(data)
     })*/

    /*   val userClicks = events.map(x => {
         println("xxxxxxxxxxxxx")
         println(x)
         println(x.getClass)
         (x.getString("uid"), x.getLong("click_count"))
       }) .reduceByKey(_ + _)*/

    /* userClicks.foreachRDD(rdd => {
       val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
       import spark.implicits._
       val word = rdd.toDF("v1", "v2")
       word.createOrReplaceTempView("words")
       val wordCount = spark.sql("select v1,sum(v2) from words group by v1")
       wordCount.show()
     })*/
    /*  userClicks.foreachRDD(rdd => {
        rdd.foreachPartition(partitions => {
          partitions.foreach(records => {
            val uid = records._1
            val clickCount = records._2
            println("uid=" + uid + " clickCount" + clickCount)
          })
        })
      })*/
    // val kafkaData = KafkaUtils.createStream(ssc, "holiday-s:2181", "test", topic).map(_._2)
    //val kafkaResult = kafkaData.flatMap(x=>x.split(",")).map(x => (x, 1)).reduceByKey(_ + _)
    kafkaData.foreachRDD(rdd => {
      rdd.foreachPartition(partitions => {
        partitions.foreach(records => {
          println(records._1)
          println(records._2)
        })
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
