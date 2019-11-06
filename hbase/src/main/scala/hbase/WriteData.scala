package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

object WriteData {
  private val conf: Configuration = ConfigUtil.apply.createHbaseConfig()

  def write(tableName: String, row: String, columnFamity: String, column: String, columnValue: String) = {
    val hTable = new HTable(conf, tableName)
    val put = new Put(Bytes.toBytes(row))
    put.addColumn(Bytes.toBytes(columnFamity), Bytes.toBytes(column), Bytes.toBytes(columnValue))
    hTable.put(put)
    println("写入成功")
    hTable.close()
  }

  def main(args: Array[String]): Unit = {
    write("holiday", "001", "a1", "name", "holiday")
  }
}
