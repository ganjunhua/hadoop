package com.ganjunhua.spark.mysql

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

object DBUtils {
  //创建文件容器
  private val prop = new Properties()
  //获取配置文件
  val dbPath = Thread.currentThread().getContextClassLoader
    .getResource("db.properties").getPath
  //配置文件加载至文件容器
  prop.load(new FileInputStream(dbPath))
  //取出配置值
  val driverName = prop.getProperty("mysqlDriverName")
  val userName = prop.getProperty("mysqlUserName")
  val password = prop.getProperty("mysqlPassword")
  val dbName = prop.getProperty("mysqlDBName")
  val port = prop.getProperty("mysqlPort")
  val dbType = prop.getProperty("mysqlDBType")
  val mysqlHost = prop.getProperty("mysqlHost")
  val url = "jdbc:" + dbType + "://" + mysqlHost + ":" + port + "/" + dbName
  classOf[com.mysql.jdbc.Driver]

  //获取连接
  def getConnection(): Connection = {
    DriverManager.getConnection(url, userName, password)
  }

  //关闭连接
  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed() || conn != null) {
        conn.close()
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }
}
