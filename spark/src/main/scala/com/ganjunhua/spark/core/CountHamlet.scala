package com.ganjunhua.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CountHamlet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hamletWordCount = sc.longAccumulator("hamletWordCount")
    val stopWordCount = sc.longAccumulator("stopWordCount")
    val hamletData = sc.textFile("data/textfile/Hamlet.txt")
    // 读取 stopWordData 数据
    val stopWordData = sc.textFile("data/textfile/stopword.txt")
    // 创建 map
    var stopWordMap: Map[String, Int] = Map()
    // stopWordData 数据 写入  stopWordMap
    stopWordData.collect().foreach(x => {
      stopWordMap += (x -> 1)
    })
    // 将 stopWordMap 作为广播变量
    val stopWordBroadCast = sc.broadcast(stopWordMap)
    val hamletRdd = hamletData.flatMap(x => x.split(" "))
      // 将单词打散
      .map(x => {
      if (x.length > 0) {
        // 去掉标点符号
        (x.toLowerCase.replaceAll("[',.:;?!-]", ""))
      } else {
        None
      }
    }).foreach(
      x => {
        // 过虑 元素为 None的
        if (x != None) {
          if (x == "ganjunhua") println(x.toString + "======19191====32044======")
          // 通过打散的单词 去广播变量里面查找。有值 则返回 1
          if (stopWordBroadCast.value.get(x.toString).size > 0) {
            stopWordCount.add(1)
          }
          // println(stopWordBroadCast.value.get(x.toString).size)

        }
      }
    )
    println(stopWordCount.value)

    sc.stop()
  }
}
