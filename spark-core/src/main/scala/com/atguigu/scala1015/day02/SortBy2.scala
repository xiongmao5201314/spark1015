package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

case class Person(age:Int,name:String)
object SortBy2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortBy2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Person(10, "lisi") :: Person(20, "zs") :: Person(15, "ww") :: Nil)
    implicit val od = new Ordering[Person] {
      override def compare(x: Person, y: Person): Int = x.age -y.age
    }
    val rdd2 = rdd1.sortBy(x => x)
    rdd2.collect().foreach(println)
    Thread.sleep(100000)
    sc.stop()
  }
}
