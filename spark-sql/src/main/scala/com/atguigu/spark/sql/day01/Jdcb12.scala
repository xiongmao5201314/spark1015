package com.atguigu.spark.sql.day01

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object Jdcb12 {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("Jdcb12").master("local[2]").getOrCreate()
    import  spark.implicits._
    val url ="jdbc:mysql://hadoop106:3306/gmall"
    val user ="root"
    val password ="123321"
/*    val df = spark.read
      .option("url",url )
      .option("user", user)
      .option("password", password)
      .option("dbtable", "test")
      .format("jdbc").load()*/
/*val props = new Properties()
    props.put("user",user)
    props.put("password",password)
    val df = spark.read.jdbc(url, "test", props)*/
val df = spark.read.json("d:/json.txt")
    /*df.write
      .format("jdbc")
      .option("url",url )
      .option("user", user)
      .option("password", password)
      .option("dbtable", "jdbc")
      .mode(SaveMode.Append)
        .save()*/
    val props = new Properties()
    props.put("user",user)
    props.put("password",password)
    df.write.jdbc(url,"1016te",props)
    df.show()
    spark.close()
  }
}
