package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession
case class User(name:String,age:Int)
object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("RDD2DF").master("local[2]").getOrCreate()
    import  spark.implicits._
    val sc = spark.sparkContext
   /* val rdd = sc.parallelize(1 to 10)
    val df = rdd.toDF()
    val rdd1 = df.rdd
    val rdd2 = rdd1.map(row => row.getInt(0))*/
   val df = spark.read.json("d:/json.txt")
    df.printSchema()
    val rdd2 = df.rdd.map(row => User(row.getString(0),row.getLong(1).toInt))

    rdd2.collect.foreach(println)
//val rdd = sc.parallelize(("lisi", 10) :: ("zs", 20) :: Nil)
/*val rdd = sc.parallelize(Array(User("adad", 10), User("za", 12)))
//    val df = rdd.toDF()
    df.show()*/
    spark.stop
  }
}
