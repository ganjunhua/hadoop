package com.ganjunhua.spark.sparksql

import org.apache.spark.sql.SparkSession

object Wenshu {
  def main(args: Array[String]): Unit = {
    var master="local[*]"
    var appName=this.getClass.getSimpleName
    var dataPath="D:\\Holiday\\Work\\BJ\\东交\\DW\\数据入库\\文书.txt"
    if(args.length==3){
      master=args(0)
      appName=args(1)
      dataPath=args(2)
    }
    val spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
    val wsData= spark.read.json(dataPath)
    wsData.printSchema()
    spark.stop()
  }
}
