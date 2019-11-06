package com.qax.yc.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql.EsSparkSQL

object StreamingSocketTextStream {

  case class Socket(msgType: String,
                    deviceId: String,
                    longti: String,
                    lati: String,
                    speed: String,
                    direction: String,
                    dateTime: String,
                    deviceType: String,
                    PDT_NO: String,
                    Police_ID: String,
                    unit_id: String
                   )


  def main(args: Array[String]): Unit = {
    var seconds = 10
    var ipAddr = "holiday-1"
    var port = 20000
    var setMaster = "local[*]"
    var indexName = "holiday/hua"
    var esClusterName = "es"
    var esNodes = "holiday"
    var esPort = "9200"
    var esUser = "holiday"
    var esPasswd = "holiday"
    if (args.length > 2) {
      seconds = args(0).toInt
      ipAddr = args(1)
      port = args(2).toInt
      setMaster = args(3)
      indexName = args(4)
      esClusterName = args(5)
      esNodes = args(6)
      esPort = args(7)
      esUser = args(8)
      esPasswd = args(9)
    }
    val sparkConf = new
        SparkConf()
      .setMaster(setMaster)
      .setAppName(this.getClass.getSimpleName)
      .set("cluster.name", esClusterName)
      .set("es.index.auto.create", "true")
      .set("es.nodes", esNodes)
      .set("es.port", esPort)
      .set("es.index.read.missing.as.empty", "true")
      .set("es.net.http.auth.user", esUser) //访问es的用户名
      .set("es.net.http.auth.pass", esPasswd) //访问es的密码
      .set("es.nodes.wan.only", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(seconds))
    val lines = ssc.socketTextStream(ipAddr, port)
    lines.print(10)
    val jsonLines = lines.map(json => {
      var msgType: Any = null
      var deviceId: Any = null
      var longti: Any = null
      var lati: Any = null
      var speed: Any = null
      var direction: Any = null
      var dateTime: Any = null
      var deviceType: Any = null
      var PDT_NO: Any = null
      var Police_ID: Any = null
      var unit_id: Any = null
      try {
        val jsonObject = JSON.parseObject(json)
        try {
          msgType = jsonObject.getOrDefault("msgType", null)
        }
        catch {
          case e: Exception => println(e)
            if (msgType == null) msgType = 99
        } finally {
          if (msgType == null || msgType == 99) {
            msgType = 99
          }
        }
        try {
          deviceId = jsonObject.getOrDefault("deviceId", null)
        } catch {
          case e: Exception => println(e)
            if (deviceId == null) deviceId = "deviceId"
        } finally {
          if (deviceId == null || deviceId == "deviceId") {
            deviceId == "deviceId"
          }
        }
        try {
          longti = jsonObject.getOrDefault("longti", null)
        } catch {
          case e: Exception => println(e)
            if (longti == null) longti = 110.110
        } finally {
          if (longti == null || longti == 110.110) {
            longti == 110.110
          }
        }

        try {
          lati = jsonObject.getOrDefault("lati", null)
        } catch {
          case e: Exception => println(e)
            if (lati == null) lati = 110.110
        } finally {
          if (lati == null || lati == 110.110) {
            lati = 110.110
          }
        }
        try {
          speed = jsonObject.getOrDefault("speed", null)
        } catch {
          case e: Exception => println(e)
            if (speed == null) speed = 0
        } finally {
          if (speed == null || speed == 0) {
            speed = 0
          }
        }
        try {
          direction = jsonObject.getOrDefault("direction", null)

        } catch {
          case e: Exception => println(e)
            if (direction == null) direction = 110
        } finally {
          if (direction == null || direction == 110) {
            direction = 110
          }
        }
        try {
          dateTime = jsonObject.getOrDefault("dateTime", null)
        } catch {
          case e: Exception => println(e)
            if (dateTime == null) dateTime = "2018-01-01 01:01:01"
        } finally {
          if (dateTime == null || dateTime == "2018-01-01 01:01:01") {
            dateTime = "2018-01-01 01:01:01"
          }
        }
        try {
          deviceType = jsonObject.getOrDefault("deviceType", null)
        } catch {
          case e: Exception => println(e)
            if (deviceType == null) deviceType = "deviceType"
        } finally {
          if (deviceType == null || deviceType == "deviceType") {
            deviceType = "deviceType"
          }
        }
        try {
          PDT_NO = jsonObject.getOrDefault("PDT_NO", null)
        } catch {
          case e: Exception => println(e)
            if (PDT_NO == null) PDT_NO = "123456"
        } finally {
          if (PDT_NO == null || PDT_NO == "123456") {
            PDT_NO = "123456"
          }
        }
        try {
          Police_ID = jsonObject.getOrDefault("Police_ID", null)
        } catch {
          case e: Exception => println(e)
            if (Police_ID == null) Police_ID = "123456"
        } finally {
          if (Police_ID == null || Police_ID == "123456") {
            Police_ID = "123456"
          }
        }
        try {
          unit_id = jsonObject.getOrDefault("unit_id", null)
        } catch {
          case e: Exception => println(e)
            if (unit_id == null) unit_id = "123456"
        } finally {
          if (unit_id == null || unit_id == "123456") {
            unit_id = "123456"
          }
        }
        (msgType, deviceId, longti, lati, speed, direction, dateTime, deviceType, PDT_NO, Police_ID, unit_id)
      } catch {
        case e: Exception => println(e)
          (msgType, deviceId, longti, lati, speed, direction, dateTime, deviceType, PDT_NO, Police_ID, unit_id)
      } finally {
        (msgType, deviceId, longti, lati, speed, direction, dateTime, deviceType, PDT_NO, Police_ID, unit_id)
      }
    })
    try {
      jsonLines.foreachRDD(rdd => {
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val socketLines = rdd
          .map(x => Socket(x._1.toString,
            x._2.toString,
            x._3.toString,
            x._4.toString,
            x._5.toString,
            x._6.toString,
            x._7.toString,
            x._8.toString,
            x._9.toString,
            x._10.toString,
            x._11.toString))
          .toDF()
        socketLines.printSchema()
        socketLines.createOrReplaceTempView("word")
        val wordMessage = spark.sql("select md5(concat(unit_id,Police_ID)) uuid_key, msgType, deviceId, longti, lati, speed, direction, dateTime, deviceType, PDT_NO, Police_ID, unit_id from word")
        EsSparkSQL.saveToEs(wordMessage, indexName, Map("es.mapping.id" -> "uuid_key"))
        wordMessage.show(10)
      })
    } catch {

      case ex: Exception => println("无数据")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}