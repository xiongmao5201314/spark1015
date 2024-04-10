package com.atguigu.scala1015.day02

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PartitionDemo2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20, null, null,'a','b','c')
    val rdd1 = sc.parallelize(list1, 4).map((_, 1))
    val rdd2 = rdd1.partitionBy(new MyPartition(2))
    rdd2.glom().map(_.toList).collect().foreach(println)
    sc.stop()
  }
}

class MyPartition (num:Int) extends Partitioner{
  assert(num > 0)
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match{

    case null => 0
    case _ => key.hashCode().abs % num
  }
}