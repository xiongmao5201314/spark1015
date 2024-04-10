package com.atguigu.scala1015.day02

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerDemo").setMaster("local[2]")
      .set("spark.serializer",classOf[Kryo].getName)
      .registerKryoClasses(Array(classOf[Searcher]))
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
    val searcher = new Searcher("hello")
    val result = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)
  }
}

private class Searcher(val query: String)  extends  Serializable {
  def isMatch(s:String)={
    s.contains(query)
  }

  def getMatchedRDD1(rdd: RDD[String])={
    val q= query
    rdd.filter(isMatch)
  }
}