package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object SortbyKey2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Double1").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val rdd = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))
//    val rdd2 = rdd.sortByKey(false)
val rdd1 = sc.parallelize(Array((1, 10),(2, 20),(4, 100),(3, 30)),1)
    val rdd2 = sc.parallelize(Array((1, "a"),(2, "b"),(1, "aa"),(3, "c")),1)

//    val rdd3 = rdd1.join(rdd2)
val rdd3 = rdd1.fullOuterJoin(rdd2)
    rdd3.collect().foreach(println)
    sc.stop()

  }
}
