package com.ganjunhua.spark.mysql

object MysqlOperations {
  def main(args: Array[String]): Unit = {
    val op = Operations.apply
    val id = "4"
    val name = "holiday"
    val age = 18
    val user = op.User(id, name, age)
    //op.add(user)
    val x = op.query(1)
    for (i <- 0 until x.length) {
      //获取数组i所有的x(i).map(x=>x._1) 返回的是list
      val keys = x(i).map(x => x._1)
      keys.foreach(key => {
        val value = x(i).get(key)
        println(key + "->" + value)
      })
      for (key <- x(i).keys) {
        //取map的值 x(i).get(key)
        //println(key + "->" + x(i).get(key))
      }
    }
  }
}
