package spark.shanghai.sql

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

/*
author:ganjunhua
date:2019-04-09
function: enableHive  saprk sql  write  es
 */
object SparkHiveToES {

  def main(args: Array[String]): Unit = {
    //remote link
    var setMetastoreuris = "thrift://10.95.42.46:9083"
    // table name
    var setTable = "edw.testsql"
    // hive node
    var setHiveHost = "127.0.0.1"
    // es node
    var setESHost = "127.0.0.1"
    // spark-warehose path
    var setWarehouseLocation = new File("spark-warehose")
      .getAbsolutePath
    //md5 field
    var setMd5 = "sex,7"
    // variable flush value
    if (args.size == 5) {
      setMetastoreuris = args(0)
      setTable = args(1)
      setHiveHost = args(2)
      setESHost = args(3)
      setMd5 = args(4)
    }
    // craete  sparkSession
    val spark = createSparkSession(setWarehouseLocation, setMetastoreuris, setESHost)
    // get index name
    val indexName = setTable.substring(4)
    // split joint sql
    val sql = s"select *,md5(concat($setMd5)) mdf from $setTable"
    //execute sql
    val dataDF = spark.sql(sql)
    //test
    dataDF.show(10)
    dataDF.printSchema()
    //write es
    saveES(dataDF, indexName)
    spark.stop()
  }

  def saveES(dataDF: DataFrame, indexName: String): Unit = {
    EsSparkSQL.saveToEs(dataDF, s"$indexName/$indexName", Map("es.mapping.id" -> "mdf"))
  }

  def createSparkSession(setWarehouseLocation: String, setMetastoreuris: String, setESHost: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .config("spark.sql.warehouse.dir", setWarehouseLocation)
      .config("hive.metastore.uris", setMetastoreuris)
      .config("cluster.name", "es")
      .config("es.nodes", s"http://$setESHost")
      .config("es.port", "9200")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.nodes.wan.only", "true")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
}
