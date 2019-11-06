package com.ganjunhua.spark.scala.training

import com.ganjunhua.spark.scala.training.`class`.constructor

class training {
  println(training.xx)
  var ab = 1

  def this(namea: Int) {
    this()
    this.ab = namea.toInt
  }
}

object training {
  var xx = 0

  def apply(name: Int) = new training(name)

  def sayHello = (name: String) => println(name)

  def sayHello1 = (name: String, age: Int) => println(name)

  def say(func: (String, Int) => Unit, xx: String): Unit = {
    func(xx, 1)
  }

  def main(args: Array[String]): Unit = {

    val list = List(1, 2, 3, 4)
    list.head

    var list1 = scala.collection.mutable.LinkedList(1, 2, 3, 4)
    var cu = list1

    val a = Set(1,2,3,3,3)
    println(a)
  }
}

