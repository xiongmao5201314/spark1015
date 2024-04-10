package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}

object Practice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val readline = sc.textFile("G:\\agent.log")
    val rdd2 = readline.map(line => {
      val split = line.split(" ")
      ((split(1), split(4)), 1)
    })
    val rdd3 = rdd2.reduceByKey(_ + _)

    val rdd4 = rdd3.map({
      case ((pro, ads), count) => (pro, (ads, count))
    })

    val rdd5 = rdd4.groupByKey()

    val resukt = rdd5.map({
      case (pro, adsCountIt) =>
        (pro, adsCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    })
    resukt.collect().foreach(println)
    sc.stop()
  }

}
