package com.ganjunhua.spark.json

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.{SaveMode, SparkSession}

object HdfsJson {
  def main(args: Array[String]): Unit = {

    val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("user", "holiday")
      .getOrCreate()
    val path = "hdfs://holiday:9000/home/tsgz-situation/ThreatSensor/" + currentDate + "/04/part-00990.gz"
    val jsonRDD = spark.read.format("json").load(path)
    jsonRDD.createOrReplaceTempView("iptable")
    val sqlString = "select ip,time ,mid  from iptable where (" +
      "substr(ip,1,3)<>'10.'" +
      " 	and  not ip   rlike   '^192.168.(1d{2}|2[0-4]d|25[0-5]|[1-9]d|[0-9]).(1d{2}|2[0-4]d|25[0-5]|[1-9]d|[0-9])$'" +
      "   and  not ip   rlike  '^172.(1[6789]|2[0-9]|3[01]).(1d{2}|2[0-4]d|25[0-5]|[1-9]d|[0-9]).(1d{2}|2[0-4]d|25[0-5]|[1-9]d|[0-9])$' )"


    val sqlResult = spark.sql(sqlString)
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "admin123")
    sqlResult.show(50)
    sqlResult.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/holiday", "ipduplicateremoval", prop)

    jsonRDD.printSchema()
    spark.stop()
  }
}
