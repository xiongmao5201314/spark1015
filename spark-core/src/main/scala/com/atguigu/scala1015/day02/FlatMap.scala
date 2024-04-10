package com.atguigu.scala1015.day02

import org.apache.spark.{SparkConf, SparkContext}
case class User(age:Int,name:String){
  override def hashCode(): Int = this.age
  override def equals(obj: Any): Boolean = obj match {
    case User(age,_) => this.age == age
    case _ => false
  }
}
object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionWithIndex").setMaster("local[4]")
    val sc = new SparkContext(conf)
 /*   val list1 = List(1 to 3, 1 to 5, 10 to 20)*/
    val list1 = List("aad","dadf","fasa","a","adf","da")
    val rdd1 = sc.parallelize(list1, 2)
//    val rdd1 = sc.parallelize(List(User(10,"li"),User(20,"za"),User(10,"ab")),2)
/*    val rdd2 = rdd1.flatMap(x => {
      List(x, x * x, x * x * x)
    })*/
//val rdd2 = rdd1.flatMap(x => if (x % 2 == 0) List(x, x * x, x * x * x) else List())
//val rdd2 = rdd1.filter(x => x > 2)
//val rdd2 = rdd1.glom().map(x => x.toList)
/*val rdd2 = rdd1.groupBy(x => x % 2)
    val rdd3 = rdd2.map {
      case (k, v) => (k, v.sum)
    }*/
//val rdd3 = rdd1.sample(true, 2)
/*implicit val od:Ordering[User] = new Ordering[User] {
  override def compare(x: User, y: User): Int = x.age -y.age
}*/
  /*  println(rdd1.getNumPartitions)
    val rdd2 = rdd1.repartition(6)
    println(rdd2.getNumPartitions)
/*val rdd3 = rdd1.distinct(2)
    rdd3.collect().foreach(println)*/
    */
val rdd2 = rdd1.sortBy(_.length,false)
    rdd2.collect().foreach(println)
    sc.stop()

  }
}
