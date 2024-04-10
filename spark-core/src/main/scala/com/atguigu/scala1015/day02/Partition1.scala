package com.atguigu.scala1015.day02

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Partition1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Double1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    val i = 4.max(5)

    val rdd2 = rdd1.combineByKey(
      v => v,
      (c: Int, v: Int) => c + v,
      (c1: Int, c2: Int) => c1 + c2

    )
//    val rdd2 = rdd1.aggregateByKey(Int.MinValue)((u, v) => u.max(v), (u1, u2) => u1 + u2)
/*val rdd2 = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue))({
  case ((max, min), v) => (max.max(v), min.min(v))
},
  {

    case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)

  })*/
/*val rdd2 = rdd1.aggregateByKey((0, 0))({
  case ((sum, count), v) => (sum + v, count + 1)

}, {
  case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
}).mapValues({

case  (sum,count) => sum.toDouble/count

})*/
//    println(rdd2.partitioner)
//    val rdd3 = rdd2.partitionBy(new HashPartitioner(2))
//    val rdd4 = rdd3.glom()
//    println(rdd3.partitioner)
/*val rdd3 = rdd2.map {
  case (k, v) => (v, k)
}.partitionBy(new HashPartitioner(2)).map {
  case (k, v) => (v, k)
}
    rdd3.glom().collect().map(_.toList).foreach(println)*/
    rdd2.collect().foreach(println)
    println(i)
    sc.stop()
  }
}
