package com.ganjunhua.spark.ganshu

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehose")
      .getAbsolutePath
    val metastoreuris = args(0)
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", metastoreuris)
      .enableHiveSupport()
      .getOrCreate()
    println("sparkTest")
    val sql =
      """
        |select a.rybh from jingwuyun.vw_c_kx_jzpt_ryjbxxb a
        |left join jingwuyun.vw_c_kx_jzpt_ry_swxx b
        |on a.rybh = b.SWRYBH
      """.stripMargin
    val sqlDF = spark.sql(sql)
    sqlDF.printSchema()
    sqlDF.show()
    sqlDF.repartition(1)
    sqlDF.write.mode(SaveMode.Append).saveAsTable("jingwuyun.sparktest")
    spark.stop()
  }
}
