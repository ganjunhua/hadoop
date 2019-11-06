package com.ganjunhua.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CountHamlet1 {
  def main(args: Array[String]): Unit = {
val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val  sc = new SparkContext(conf)
    val hamletWordCount = sc.longAccumulator("hamlet")
    val stopWordCount = sc.longAccumulator("stopWord")
    val hamletData = sc.textFile("data/textfile/Hamlet.txt")
    val stopWordData= sc.textFile("data/textfile/stopword.txt")
    var stopWordMap:Map[String,Int] = Map()
    stopWordData.collect().foreach(x=>{
      stopWordMap += (x->1)
    })
val stopWordBroadCast = sc.broadcast(stopWordMap)
    val hamletRDD = hamletData.flatMap(x=>x.split(" "))
      .map(x=>{
        if(x.length>0){
  x.toLowerCase().replaceAll("[',.:;?!-]", "")
        }else {
          None
        }
      })
      .foreach(x=>{
        if (x !=None){
          if(stopWordBroadCast.value.get(x.toString).size>0){
            stopWordCount.add(1)
          }
        }
      })


    print("stopWordCount.add(1)"+stopWordCount.value)
  }
}
