package com.ganjunhua.spark.film

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.EsSparkSQL

object Audiences {

  case class User(userID: String, sex: String, age: Int)

  case class Rating(userID: String, movieID: String, score: Int)

  def main(args: Array[String]): Unit = {
    var master = "local[*]"
    var appName = this.getClass.getSimpleName
    var dataPath = "data/ml-1m"
    if (args.length == 3) {
      master = args(0)
      appName = args(1)
      dataPath = args(2)
    }
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      //.config("es.index.auto.create", "true")
      .config("cluster.name", "my-application")
      .config("es.nodes", "holiday-f")
      .config("es.port", "9201")
      .config("es.index.read.missing.as.empty", "true")
      .config("es.net.http.auth.user", "holiday") //访问es的用户名
      .config("es.net.http.auth.pass", "admin123") //访问es的密码
      .config("es.nodes.wan.only", "true")
      .getOrCreate()
    import spark.implicits._
    val movieID = "2116"
    val userData = spark.read.textFile(dataPath + "/users.dat")//.as[User]
    val userDF = userData.map(x => x.split("::"))
      .map(x => User(x(0), x(1), x(2).toInt))
     // .toDF()
    val ratingsData = spark.read.textFile(dataPath + "/ratings.dat")
    val ratingDF = ratingsData.map(x => x.split("::"))
      .map(x => Rating(x(0), x(1), x(2).toInt))
      .toDF()

    ratingDF.filter(s"movieID = ${movieID} ")
      .join(userDF, "userID")
      .select("sex", "age")
      .groupBy("sex", "age")
      .agg(count('sex) as "cnt")
      .select("sex", "age", "cnt").show(10)
    userDF.filter("age >17 and age < 25 and sex ='M'")
      .join(ratingDF, "userID")
      .groupBy("userID")
      .agg(sum('score) as "sumScore")
      .orderBy(desc("sumScore"))
      .show(10)

    userDF.createOrReplaceTempView("userTempView")
    ratingDF.createOrReplaceTempView("ratingTempView")
    val esResult = spark.sql("select a.sex,a.age,count(1),md5(concat(a.sex,a.age,a.age)) mdf from userTempView a " +
      s" join ratingTempView b on a.userID=b.userID where b.movieID=${movieID}" +
      s" group by a.sex,a.age")

    EsSparkSQL.saveToEs(esResult, "film/count", Map("es.mapping.id" -> "mdf"))
    spark.stop()
  }
}
