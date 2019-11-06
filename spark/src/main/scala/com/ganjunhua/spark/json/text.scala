package com.ganjunhua.spark.json

import java.text.SimpleDateFormat
import java.util.Date

object text {
  def main(args: Array[String]): Unit = {
    val a= new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    print(a)
  }
}
