package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

class ConfigUtil {
  def createHbaseConfig(): Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    println("ConfigUtil.............")
    conf.set("hbase.zookeeper.quorum", "holiday-f:2181,holiday-s:2181,holiday-t:2181")
    conf
  }
}

object ConfigUtil {
  def apply: ConfigUtil = new ConfigUtil()

  def main(args: Array[String]): Unit = {
    ConfigUtil.apply.createHbaseConfig()
  }
}

