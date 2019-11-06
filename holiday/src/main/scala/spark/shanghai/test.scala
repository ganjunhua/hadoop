package spark.shanghai

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val dataPath = "D:\\Holiday\\Work\\SH\\分析平台对接\\155.dat"
    val jsonData = spark.sparkContext.textFile(dataPath)

    val data = jsonData.foreach(json => {
      val jsonObject = JSON.parseArray(json)
      // 创建数组容器
      var dataList: List[String] = List()
      for (i <- 0 to jsonObject.size() - 1) {
        if (i == 0) {
          //println(jsonObject.size())
          val xx = (jsonObject.get(i))
          val hash = JSON.parseObject(xx.toString).get("hash")
          val product = JSON.parseObject(xx.toString).get("product")
          val msgtype = JSON.parseObject(xx.toString).get("msgtype")
          val dev_ip = JSON.parseObject(xx.toString).get("dev_ip")
          //println(dev_ip)
          dataList = hash.toString :: product.toString :: msgtype.toString :: dev_ip.toString :: dataList
        }
        if (i == 1) {
          //println(jsonObject.size())
          val xx = (jsonObject.get(i))
          val replib = JSON.parseObject(xx.toString).get("replib")
          val engine = JSON.parseObject(xx.toString).get("engine")
          val netcard = JSON.parseObject(xx.toString).get("netcard")
          val flashver = JSON.parseObject(xx.toString).get("flashver")
          val avlib = JSON.parseObject(xx.toString).get("avlib")
          val urllib = JSON.parseObject(xx.toString).get("urllib")
          val time = JSON.parseObject(xx.toString).get("time")
          val rulelib = JSON.parseObject(xx.toString).get("rulelib")
          val agentver = JSON.parseObject(xx.toString).get("agentver")

          dataList = replib.toString :: engine.toString :: netcard.toString ::
            flashver.toString :: avlib.toString :: urllib.toString :: time.toString :: rulelib.toString :: agentver.toString :: dataList
        }
        // 将数组添加至dataList
        dataList
      }
println(dataList)
      dataList.toString()
    })//.map(x => JSON.parseObject(x))
  //  import spark.implicits._
  //  data.foreach(println(_))
    // flatMap(x =>x.split(",")).foreach(println(_))
    //.map(x => JSON.parseObject(x))
    /* .toDF("replib","engine","netcard","flashver","avlib",
     "urllib","time","rulelib","agentver","hash","product","msgtype","dev_ip")

   data.createOrReplaceTempView("a")
   spark.sql("select * from a").show(10)
*/

    spark.stop()
  }
}
