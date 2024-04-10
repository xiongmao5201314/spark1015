package com.atgui.scala1015.day01

import org.apache.spark.{SparkConf, SparkContext}


object Hello {
  def main(args: Array[String]): Unit = {
    //1.创建一个sparkContext 打包时候去掉setMaster
    val conf = new SparkConf().setAppName("Hello").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //2.从数据源得到一个RDD
    val lineRDD = sc.textFile(args(0))
    //3.对RDD做各种转换
    val resultRDD = lineRDD.flatMap(_.split("\\W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
    //4.执行一个行的算子
    val wordCountArr = resultRDD.collect()
    wordCountArr.foreach(println)
    //5.关闭sparkContext
    sc.stop()
  }
}
