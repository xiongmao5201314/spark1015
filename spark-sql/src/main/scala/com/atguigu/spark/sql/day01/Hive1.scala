package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

object Hive1 {
  def main(args: Array[String]): Unit = {
//    System.setProperties("HADOOP_USER_NAME","atguigu")
    val spark =
      SparkSession.builder()
        .appName("Hive1")
        .master("local[2]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://hadoop104:9000/user/hive/warehouse")
        .getOrCreate()
    import  spark.implicits._
    /*spark.sql("show databases")
    spark.sql("use gmall")
    spark.sql("select count(*) ads_uv_count").show()*/
//    spark.sql("create database spark1015").show()
val df = spark.read.json("d:/json.txt")
    spark.sql("use spark1015")
//    spark.sql("create table user1(id int,name string)").show
//    spark.sql("insert into user1 values(10,'list1')").show
//    df.write.mode("append")saveAsTable("user2")
    df.write.insertInto("user2")

    spark.close()
  }
}
