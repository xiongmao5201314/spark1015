package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object Day01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day01").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70,60,10,20)
//   val rdd1 = sc.parallelize(Array(("a", 10), ("a", 20), ("b", 100), ("c", 200)))
    val rdd1 = sc.parallelize(list1,2)
//    val rdd2 = rdd1.takeOrdered(3)(Ordering.Int.reverse)
//val rdd2 = rdd1.countByKey()
    rdd1.foreach(x => println(x))
//    println(rdd2)
    sc.stop()
  }
}
