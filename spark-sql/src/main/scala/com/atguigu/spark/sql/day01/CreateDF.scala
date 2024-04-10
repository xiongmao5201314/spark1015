package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

object CreateDF {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("CreateDF").master("local[2]")getOrCreate()
    val df = spark.read.json("d:/json.txt")
    df.createOrReplaceTempView("user")
    spark.sql(
      """
        |select *
        |from
        |user
        |""".stripMargin).show()
    spark.stop()
  }
}
