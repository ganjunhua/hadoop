package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{HTable, Scan}

object ScanTable {
  private val conf: Configuration = ConfigUtil.apply.createHbaseConfig()

  def scan(tableName: String) = {
    val hTable = new HTable(conf, tableName)
    val result = hTable.getScanner(new Scan()).iterator()
    while (result.hasNext) {
      val data = result.next()
      for (kv <- data.rawCells()) {
        println(new String(CellUtil.cloneRow(kv)))
        println(new String(CellUtil.cloneValue(kv)))
        println(new String(CellUtil.cloneFamily(kv)))

      }
    }
  }

  def main(args: Array[String]): Unit = {
    scan("holiday")
  }
}
