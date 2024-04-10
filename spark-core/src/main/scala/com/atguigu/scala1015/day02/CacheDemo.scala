package com.atguigu.scala1015.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")
    val rdd1 = sc.parallelize(Array("ab", "bc"))
    val rdd2 = rdd1.flatMap(x => {
      println("flatMap...")
      x.split("")
    })
    val rdd3: RDD[(String, Int)] = rdd2.map(x => {
      (x, 1)
    })
//    val rdd4 = rdd3.reduceByKey(_ + _)
//    rdd3.persist(StorageLevel.MEMORY_ONLY)
//    rdd3.cache()
    rdd3.checkpoint()
    rdd3.cache()
    rdd3.collect.foreach(println)
    println("-----------")
    rdd3.collect.foreach(println)
    Thread.sleep(1000000)
  }

}
