package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object Double1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Double1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 80, 60, 20,90, 60)
    val list2 = List(30, 5, 7, 60, 1, 2)
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = sc.parallelize(list2, 2)
//    val rdd3 = rdd1.union(rdd2)
//val rdd3 = rdd1 ++ rdd2
//val rdd3 = rdd1.intersection(rdd2)
//val rdd3 = rdd1.subtract(rdd2)
//val rdd3 = rdd1.cartesian(rdd2)
//val rdd3 = rdd1.zip(rdd2)
/*val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => {
  it1.zipAll(it2,12,14)
})*/
val rdd3 = rdd1.zipWithIndex()
    rdd3.collect().foreach(println)


    sc.stop()
  }
}
