package com.atguigu.scala1015.day02

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1 = sc.parallelize(list1, 2)
//    val acc = sc.longAccumulator
    val acc = new MapAcc
     sc.register(acc, "first")
    var a =0
    /*val rdd2 = rdd1.map(x => {
      acc.add(1)
      a += 1
      x
    })*/
    val rdd2 = rdd1.foreach(x => acc.add(x))

//    println(a)
    println(acc.value)
    sc.stop()
  }
}
class MapAcc extends AccumulatorV2[Double,Map[String,Any]] {
  private var map: Map[String, Any] = Map[String, Any]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Double, Map[String, Any]] = {
    val acc = new MapAcc
    acc.map = map
    acc
  }

  override def reset(): Unit = map = Map[String, Any]()

  override def add(v: Double): Unit = {
    map += "sum" -> (map.getOrElse("sum",0D).asInstanceOf[Double] + v)
    map += "count" -> (map.getOrElse("count",0L).asInstanceOf[Long]+ 1L)
  }

  override def merge(other: AccumulatorV2[Double, Map[String, Any]]): Unit = {
    other match {
      case o : MapAcc => map +=
        "sum" -> (map.getOrElse("sum",0D).asInstanceOf[Double] +o.map.getOrElse("sum",0D).asInstanceOf[Double])
        map +=
          "count" -> (map.getOrElse("count",0L).asInstanceOf[Long] +o.map.getOrElse("count",0L).asInstanceOf[Long])

    }
  }

  override def value: Map[String, Any] = {
    map += "avg" -> map.getOrElse("sum",0D).asInstanceOf[Double]/map.getOrElse("count",0L).asInstanceOf[Long]
    map
  }
}
/*
class MyIntAcc extends AccumulatorV2[Int,Int] {
  private var sum =0
  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyIntAcc
    acc.sum = sum
    acc
  }

  override def reset(): Unit = sum =0

  override def add(v: Int): Unit = sum += v

  override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
    case acc :MyIntAcc => this.sum += acc.sum
    case _ => this.sum += 0
  }

  override def value: Int = sum
}*/
