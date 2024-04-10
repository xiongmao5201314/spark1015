package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object BroadCast2 {
  def main(args: Array[String]): Unit = {
    val bigArr = 1 to 1000 toArray
    val conf = new SparkConf().setAppName("BroadCast2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 500000, 70, 600000, 10, 20)
    val bd = sc.broadcast(bigArr)
    val rdd1 = sc.parallelize(list1, 4)
    val rdd2 = rdd1.filter(x => bd.value.contains(x))
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
