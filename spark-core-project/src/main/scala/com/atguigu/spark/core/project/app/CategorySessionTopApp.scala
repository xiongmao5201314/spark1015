package com.atguigu.spark.core.project.app



import com.atguigu.spark.core.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CategorySessionTopApp {
  def statCategorySessionTop10(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction])={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    val cidSidAndOne = filteredUserVistActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(_ + _)
    val cidAndSidCount = cidSidAndCount.map(action => (action._1._1, (action._1._2, action._2)))
    val cidAndSidCountItRDD = cidAndSidCount.groupByKey()
    val result = cidAndSidCountItRDD.mapValues(it => it.toList.sortBy(-_._2).take(10))
    result.collect().foreach(println)

  }
  def statCategorySessionTop10_2(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction])={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    val cidSidAndOne = filteredUserVistActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(_ + _)
    val cidAndSidCount = cidSidAndCount.map{
      case ((cid, sid), count) => (cid, (sid, count))
    }
    val cidAndSidCountItRDD = cidAndSidCount.groupByKey()
    val result = cidAndSidCountItRDD.mapValues( (it: Iterable[(String, Int)]) => {
      var set = mutable.TreeSet[SessionInfo]()
      it.foreach {
        case (sid,count) =>
          val info = SessionInfo(sid, count)

          set += info
          if(set.size > 10)   set.take(10)
      }
      set.toList
    })
    result.collect().foreach(println)

  }

  def statCategorySessionTop10_1(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction])={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    val temp = cids.map(cid => {
      val cidUserVisitActionRDD = filteredUserVistActionRDD.filter(action => (action.click_category_id == cid))
      cidUserVisitActionRDD

      val r = cidUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        .reduceByKey(_ + _)
        .map {
          case ((cid, sid), count) => (cid, (sid, count))
        }
        .sortBy(-_._2._2)
        .take(10)
        .groupBy(_._1)
        .map {
          case (cid, arr) => (cid, arr.map(_._2).toList)
        }
      r
    })
    val result = temp.flatMap(map => map)
    result.foreach(println)
  }
  def statCategorySessionTop10_3(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction])={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    val cidSidAndOne = filteredUserVistActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(new CategorySessionPartitioner(cids),_ + _)
    val result=cidSidAndCount.mapPartitions(it => {
      var set: mutable.TreeSet[SessionInfo] = mutable.TreeSet[SessionInfo]()
      var categoryId = -1L
      it.foreach {
        case ((cid,sid), count) =>
          categoryId = cid
          val info = SessionInfo(sid, count)
          set += info
          if (set.size > 10) set = set.take(10)
      }
//      set.map((categoryId,_)).toIterator
      Iterator((categoryId,set.toList))
    })
  result.collect.foreach(println)
  }
}

class CategorySessionPartitioner(cids:Array[Long]) extends Partitioner {
  private val index1 = cids.zipWithIndex.toMap
  override def numPartitions: Int = cids.length

  override def getPartition(key: Any): Int = key match {
    case (cid:Long , _ ) => index1(cid)
  }
}
