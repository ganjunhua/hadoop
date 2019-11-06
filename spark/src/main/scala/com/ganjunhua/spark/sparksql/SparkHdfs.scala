package com.ganjunhua.spark.sparksql

import org.apache.spark.sql.SparkSession

object SparkHdfs {

  case class Ryjbxxb(rybh: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    var hdfsPath: String = null
    var tableName: String = null
    import spark.implicits._
    if (args.length <= 0) {
      hdfsPath = "hdfs://holiday-1:8020/user/hive/warehouse/jingwuyun.db/vw_c_kx_jzpt_ryjbxxb/vw_c_kx_jzpt_ryjbxxb_*"
      tableName = "ryjbxxb"
    } else {
      hdfsPath = args(0)
    }
    val ryjbxxbData = spark.sparkContext.textFile(hdfsPath)
    val ryjbxxbDF = ryjbxxbData.map(x => x.split(","))
      .map(x => Ryjbxxb(x(0))).toDF()
    ryjbxxbDF.createOrReplaceTempView(tableName)
    val sql = "select * from  " + tableName
    spark.sql(sql).show()
    spark.stop()
  }
}
