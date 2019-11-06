package com.ganjunhua.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CountHtml1 {
  def main(args: Array[String]): Unit = {
    var master="local[*]"
    var appName=this.getClass.getSimpleName
    var dataPath ="data/access.log"
    if (args.length == 3){
      master=args(0)
      appName=args(1)
      dataPath=args(2)
    }
    val conf  = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val sc = new SparkContext(conf)
    var count404=sc.longAccumulator("404")
    var count200=sc.longAccumulator("202")
    var countSum=sc.longAccumulator("Sum")
    val dataRDD = sc.textFile(dataPath)
    dataRDD.foreach(x=>{
      val line = x.split(",")
      if (line(0)=="404") count404.add(1)
      if (line(0)=="200") count200.add(1)
      countSum.add(1)
    })
print("404="+count404.value+" 200="+count200.value+" sum="+countSum.value)
    sc.stop()
  }
}
