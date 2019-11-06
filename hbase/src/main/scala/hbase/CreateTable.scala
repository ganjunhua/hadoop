package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin

object CreateTable {
  private val conf: Configuration = ConfigUtil.apply.createHbaseConfig()

  def createTabel(tableName: String, columnFamailys: Array[String]): Unit = {
    val admin = new HBaseAdmin(conf)
    if (admin.tableExists(tableName)) {
      println("表" + tableName + "已经存在")
      return
    }
    val tableDec = new HTableDescriptor(tableName)
    for (columnFaily <- columnFamailys) {
      tableDec.addFamily(new HColumnDescriptor(columnFaily))
    }
    admin.createTable(tableDec)
    println(tableName + "创建成功")
  }

  def main(args: Array[String]): Unit = {
    CreateTable.createTabel("holiday", Array("a1", "b1"))
  }
}
