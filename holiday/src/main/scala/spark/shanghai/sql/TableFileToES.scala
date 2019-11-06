package spark.shanghai.sql

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

object TableFileToES {

  case class testsql(id: String)

  def main(args: Array[String]): Unit = {
    var dataPath = "hdfs://127.0.0.1:9000/home/hive/hive/warehouse/edw.db/testsql2/*"
    var setESHost = "127.0.0.1"
    val spark = SparkSession
      .builder().config("cluster.name", "es")
      .config("es.nodes", s"http://$setESHost")
      .config("es.port", "9200")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.nodes.wan.only", "true")
      .master("local[*]").getOrCreate()
    val hiveData = spark.sparkContext.textFile(dataPath)
    import spark.implicits._
    val dataDF = hiveData.map(x => x.split(",")).map(x => testsql(x(0))).toDF("id")
    dataDF.createOrReplaceTempView("xx")
    val hiveDF = spark.sql("select * from xx")
    EsSparkSQL.saveToEs(hiveDF, "testsql2/testsql2")
    spark.stop()
  }
}
