package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("d:/user_visit_action.txt")
    val userVisitActionRDD = sourceRDD.map(line => {
      val splits = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //需求1
    val categoryTop10 = CategoryTopApp.calcCategoryTop10(sc, userVisitActionRDD)
//    userVisitActionRDD.collect().foreach(println)
//    CategorySessionTopApp.statCategorySessionTop10_3(sc, categoryTop10,userVisitActionRDD)
    PageConversion.statPageConversionRate(sc,userVisitActionRDD,"1,2,3,4,5,6,7" )
    sc.stop()
  }
}
