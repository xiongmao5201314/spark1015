package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}


object MapPartitionDeom1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionDeom1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1 = sc.parallelize(list1,2)
    val rdd2 = rdd1.mapPartitions((it:Iterator[Int]) => {
      println("abc")
      it.map(_ * 2)
    })
    rdd2.collect.foreach(println)
    sc.stop()
  }

}
