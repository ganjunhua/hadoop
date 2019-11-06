package spark.shanghai.sql


import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

/*
author:ganjunhua
date:2019-04-09
function: enableHive  saprk sql  write  es
 */
object HdfsToES {

  def main(args: Array[String]): Unit = {
    // set master
    var setMaster = "local[*]"
    // table name
    var setTable = "testes"
    // hive node sql("use tmp_test")
    // es node
    var setESHost = "127.0.0。1"
    //md5 field
    var setMd5 = "sex,sex,sex,8111"
    // set Database
    var setDatabase = "edw"
    // variable flush value
    var setCondition_field = "1"
    var setCondition_value = "1"
    var setClusterName = "es"
    if (args.size > 4) {
      setTable = args(0)
      setESHost = args(1)
      setMd5 = args(2)
      setDatabase = args(3)
      setClusterName = args(4)
      if (args.size == 7) {
        setCondition_field = args(5)
        setCondition_value = args(6)
      }
    }
    // craete  sparkSession
    val spark = createSparkSession(setMaster, setESHost, setClusterName)
    // split joint sql Table or view not found
    spark.sql(s"use $setDatabase")
    val sql = s"select *,md5(concat($setMd5)) mdf from $setTable where $setCondition_field=$setCondition_value"

    //execute sql
    val dataDF = spark.sql(sql)
    //test
    dataDF.show(10)
    dataDF.printSchema()
    //write es
    saveES(dataDF, setTable)
    spark.stop()
  }

  def saveES(dataDF: DataFrame, indexName: String): Unit = {
    EsSparkSQL.saveToEs(dataDF, s"$indexName/$indexName", Map("es.mapping.id" -> "mdf"))
  }

  def createSparkSession(setMaster: String, setESHost: String, setClusterName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .master(setMaster)
      .appName(this.getClass.getSimpleName)
      .config("es.index.auto.create", "true")
      .config("spark.some.config.option", "some-value")
      .config("cluster.name", setClusterName)
      .config("es.nodes", setESHost)
      .config("es.port", "9200")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.net.http.auth.user", "root") //访问es的用户名
      .config("es.net.http.auth.pass", "admin123") //访问es的密码
      .config("es.nodes.wan.only", "true")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
}
