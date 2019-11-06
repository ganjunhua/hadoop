package com.ganjunhua.spark.scala.training.`class`

class constructor {
    var name = ""
    var age = 0

  def this(name: String) {
    this()
    this.name = name
  }

  def this(name: String, age: Int) {
    this(name)
    this.age = age
  }
}
class con extends constructor{
  println(age)
  val a = new constructor()
  a match {
    case a:constructor =>println("xxx")
    case _=>println()
  }
}
