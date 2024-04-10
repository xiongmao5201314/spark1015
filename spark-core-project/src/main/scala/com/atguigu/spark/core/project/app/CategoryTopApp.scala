package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTopApp {
  def calcCategoryTop10(sc: SparkContext,userVisitActionRDD:RDD[UserVisitAction])={
    val acc = new CategoryAcc
    sc.register(acc)
    userVisitActionRDD.foreach(action => acc.add(action))
    val cidActionCountGrouped = acc.value.groupBy(_._1._1)
    val categogryCountInfoArray = cidActionCountGrouped.map({
      case (cid, map) => CategoryCountInfo(cid,
        map.getOrElse((cid, "click"), 0L),
        map.getOrElse((cid, "order"), 0L),
        map.getOrElse((cid, "pay"), 0L)

      )

    }).toArray
    val result = categogryCountInfoArray.sortBy(x => (-x.clickCount, -x.orderCount, -x.payCount)).take(10)
//    result.foreach(println)
    result
  }
}
