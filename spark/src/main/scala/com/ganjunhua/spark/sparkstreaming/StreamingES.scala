package com.ganjunhua.spark.sparkstreaming

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL

object StreamingES {
  def main(args: Array[String]): Unit = {

    var master="local[*]"
    var appName=this.getClass.getSimpleName
    var streamSeconds = 20
    var dataPath ="D:\\test\\spark\\"

    if (args.size==4){
      master = args(0)
      appName = args(1)
      streamSeconds = args(2).toInt
      dataPath = args(3)
    }

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("cluster.name", "my-application")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "holiday-f")
      .set("es.port", "9201")
      .set("es.index.read.missing.as.empty", "true")
      .set("es.net.http.auth.user", "holiday") //访问es的用户名
      .set("es.net.http.auth.pass", "admin123") //访问es的密码
      .set("es.nodes.wan.only", "true")

    val ssc = new StreamingContext(conf,Seconds(streamSeconds))
    //spark中文乱码
    //val data = ssc.sparkContext.hadoopFile(dataPath,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1)

    val data = ssc.textFileStream(dataPath)
    val words = data.flatMap(x=>x.split(","))
    val wordPair = words.map(x=>(x,1))
    wordPair.foreachRDD(rdd=>{
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val wordDF = rdd.toDF("v1","v2")
      wordDF.createOrReplaceTempView("word")
      val wordCount = spark.sql("select v1,count(1) v3 from word group by v1")

      EsSparkSQL.saveToEs(wordCount,"stream/word")

      rdd.foreachPartition(partitionOfRecords=>{
        partitionOfRecords.foreach(record=>{
          val word = record._1
          val count = record._2

          println("xxxxxxxxxxxxxxxxxxxxx")
          println(word+"="+count)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
