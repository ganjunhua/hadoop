package com.ganjunhua.spark.sparksql

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object SparkLinkHive {
  def main(args: Array[String]): Unit = {

    System.setProperty("user.name", "root")
    System.setProperty("hadoop.home.dir", "D:\\hadoop")
    val warehouseLocaltion =
      new File("spark-warehouse")
        .getAbsolutePath
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.warehouse.dir", warehouseLocaltion)
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate()
    println("xxx")
    //val sql = "select * from default.record"
    spark.sql("select * from edw.gjh").show()
    spark.stop()
  }
}
