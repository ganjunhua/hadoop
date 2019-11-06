package com.ganjunhua.spark.film

import org.apache.spark.sql.SparkSession

object SparkReadES {
  def main(args: Array[String]): Unit = {
    var master = "local[*]"
    var appName = this.getClass.getSimpleName
    if (args.length == 2) {
      master = args(0)
      appName = args(1)
    }
    val spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .config("cluster.name", "my-application")
      .config("es.nodes", "holiday-f")
      .config("es.port", "9201")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.net.http.auth.user", "holiday") //访问es的用户名
      .config("es.net.http.auth.pass", "admin123") //访问es的密码
      .config("es.nodes.wan.only", "true")
      .getOrCreate()
    val filmRDD = spark.read.format("es").load("film/count")
    filmRDD.foreachPartition(x => {
      x.foreach(x => {
        println(x)
        println(x(0))
        println(x(1))
      })
    })

    spark.stop()
  }
}
