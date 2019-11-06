package com.ganjunhua.spark.mysql


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Operations {

  case class User(id: String, name: String, age: Int)

  //写入
  def add(user: User): Unit = {
    val conn = DBUtils.getConnection()
    try {
      val sql = new StringBuilder()
        .append("insert into text_user(id,name,age)")
        .append(" values (?,?,?)")
      val pstm = conn.prepareStatement(sql.toString())
      pstm.setObject(1, user.id)
      pstm.setObject(2, user.name)
      pstm.setObject(3, user.age)
      pstm.executeUpdate()
    } finally {
      conn.close()
    }
  }

  //删除
  def delete(id: String): Unit = {
    val conn = DBUtils.getConnection()
    try {
      val sql = "delete from text_user where id =?"
      val pstm = conn.prepareStatement(sql.toString)
      pstm.setObject(1, id)
      pstm.executeUpdate()
    } finally {
      conn.close()
    }
  }

  //更新
  def modifyAge(user: User): Unit = {
    val conn = DBUtils.getConnection()
    try {
      val sql = "update text_user set age =? where id=?"
      val pstm = conn.prepareStatement(sql.toString)
      pstm.setObject(1, user.age)
      pstm.setObject(2, user.id)
      pstm.executeUpdate()
    } finally {
      conn.close()
    }
  }

  //查询
  def query(id: Int): ArrayBuffer[mutable.HashMap[String, Any]] = {
    val conn = DBUtils.getConnection()
    try {
      val sql = new StringBuilder()
        .append("select name,age")
        .append(" from text_user ")
        .append(" where id >?")
      val pstm = conn.prepareStatement(sql.toString())
      pstm.setObject(1, id)
      val result = pstm.executeQuery()
      val rsmd = result.getMetaData()
      val size = rsmd.getColumnCount()
      val buffer = new ArrayBuffer[mutable.HashMap[String, Any]]()
      while (result.next()) {
        val map = mutable.HashMap[String, Any]()
        for (i <- 1 to size) {
          map += (rsmd.getColumnLabel(i) -> result.getString(i))
        }
        buffer += map
      }
      buffer
    } finally {
      conn.close()
    }
  }
}

object Operations {
  def apply: Operations = new Operations()
}
