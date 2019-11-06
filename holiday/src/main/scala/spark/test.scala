package spark

import java.io.File
import java.util.{Date, Properties}

import org.apache.spark.sql.{SaveMode, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    var setWarehouseLocation = new File("spark-warehose")
      .getAbsolutePath
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", setWarehouseLocation)
      //.config("hive.metastore.uris", setMetastoreuris)
      .master("local[*]").enableHiveSupport().getOrCreate()
    // val dataPath = "D:\\Holiday\\Work\\SH\\分析平台对接\\e.txt"
    val dataPath = "file://D:\\1.txt"
    val d1 = spark.read.format("json").load(dataPath).toDF("acted",
      "action",
      "agentver",
      "ar",
      "avlib",
      "card",
      "date",
      "dev_ip",
      "dhcpmac",
      "dip",
      "dmac",
      "dport",
      "ds",
      "dst_asset",
      "dzonename",
      "engine",
      "flashver",
      "group",
      "hash",
      "iscdnip",
      "lasttimes",
      "module",
      "msel",
      "msg",
      "msgtype",
      "netcard",
      "product",
      "rawinfo",
      "rawlen",
      "replib",
      "ruleid",
      "rulelib",
      "sip",
      "smac",
      "smt_user",
      "sport",
      "src_asset",
      "strategy",
      "szonename",
      "time",
      "urllib",
      "user",
      "vidxxx")
    d1.printSchema()
    d1.createOrReplaceTempView("d1")
    spark.sql("select * from d1").show(10)


    val d2 = spark.sql("select msgtype,vidxxx from d1 where msgtype='30721' or (msgtype == 1 and module == 0) ")
    val url = "jdbc:hive2://127.0.0.1:10000/edw"
    val prop = new Properties()
    prop.setProperty("user", "edw")
    prop.setProperty("password", "edw")
    // d2.write.mode(SaveMode.Append).jdbc(url, "testes", prop)
     d2.write.mode(SaveMode.Append).saveAsTable("edw.testes")
    spark.stop()


  }
}