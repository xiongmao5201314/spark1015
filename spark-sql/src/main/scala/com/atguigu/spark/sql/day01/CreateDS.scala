package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession
case class User1(name:String,salary:Long)
object CreateDS {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("CreateDS").master("local[2]").getOrCreate()
    import  spark.implicits._
    val sc = spark.sparkContext
//    val list1 = List(30, 50, 60, 70, 40, 20, 10)
//val list1 = List(User("zs", 10), User("lisi", 20))
//    val ds = list1.toDS().map(user => user.name)
val df = spark.read.json("d:/json.txt")
    val ds = df.as[User1]
    val df1 = ds.toDF()
    df1.show()
    spark.stop()
  }
}
