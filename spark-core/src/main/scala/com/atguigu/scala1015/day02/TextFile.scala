package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object TextFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf = new SparkConf().setAppName("TextFile").setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.parallelize("hello world" :: "hello" :: Nil)
      .flatMap(_.split("\\W+")).map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile("/word1017")
    sc.stop()
  }
}
