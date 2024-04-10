package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionWithIndex").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1 = sc.parallelize(list1,4)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map((index, _))
    })
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
