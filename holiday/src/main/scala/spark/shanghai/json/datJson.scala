package spark.shanghai.json

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession

object datJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    val dataPath = "D:\\Holiday\\Work\\SH\\分析平台对接\\155.dat"
    val dataDF = spark.sparkContext.textFile(dataPath)
    dataDF.foreach { json => {
      val jsonObect = JSON.parseArray(json)
      //var dataList: List[String] = List()
      var dataList:List[AnyRef] = List()
      for (i <- 0 to jsonObect.size() - 1) {
        val s1 = jsonObect.get(i)
        // println(s1.toString)
       // dataList = s1.toString :: dataList
        val s2 = JSON.toJSON(s1)
        println(s2)
      }
    }
    }
    spark.stop()
  }
}
