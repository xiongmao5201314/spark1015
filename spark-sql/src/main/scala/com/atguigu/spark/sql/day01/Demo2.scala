package com.atguigu.spark.sql.day01

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
case class Dog(name:String,age :Int)
case class AgeAvg (sum :Int,count:Int){
  def avg = {
    sum.toDouble / count
  }
}
object Demo2 {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("Demo2").master("local[2]").getOrCreate()
    import  spark.implicits._
    val ds = List(Dog("大黄", 6), Dog("小黄", 2), Dog("中黄", 4)).toDS()
    val avg = new MyAvg2().toColumn.name("avg")
    val result = ds.select(avg)
    result.show()
    spark.stop()
  }
}
class MyAvg2 extends Aggregator[Dog,AgeAvg,Double] {
  override def zero: AgeAvg = AgeAvg(0,0)

  override def reduce(b: AgeAvg, a: Dog): AgeAvg = a match {
    case Dog(name,age) => AgeAvg(b.sum+age,b.count+1)
    case _ => b
  }

  override def merge(b1: AgeAvg, b2: AgeAvg): AgeAvg = {
    AgeAvg(b1.sum+b2.sum,b1.count+b2.count)
  }

  override def finish(reduction: AgeAvg): Double = reduction.avg

  override def bufferEncoder: Encoder[AgeAvg] = Encoders.product //如果是样例类返回这个就可以

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

