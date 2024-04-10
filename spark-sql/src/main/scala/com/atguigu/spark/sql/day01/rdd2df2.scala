package com.atguigu.spark.sql.day01


import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object rdd2df2 {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("rdd2df2").master("local[2]").getOrCreate()
    import  spark.implicits._
    val sc = spark.sparkContext
    val rdd = sc.parallelize(("lisi", 10) :: ("zs", 20) :: Nil)
    val rdd2 = rdd.map {
      case (name, age) => Row(name, age)
    }
    val schema =
      StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))

    val df = spark.createDataFrame(rdd2, schema)
    df.show()
    spark.stop()
  }
}
