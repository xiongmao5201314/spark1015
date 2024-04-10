package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable



class CategoryAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  self =>
  private val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    val acc = new CategoryAcc
    map.synchronized{
      acc.map ++= map

    }
    acc
  }

  override def reset(): Unit = map.clear()
  //Map[()]
  override def add(v: UserVisitAction): Unit = {
    v match {
      case action if action.click_category_id != -1 =>
        val key = (action.click_category_id.toString, "click")
      map += key -> (map.getOrElse(key, 0L ) + 1L)
      case action if action.order_category_ids != "null" =>
        val cids = action.order_category_ids.split(",")
        cids.foreach(cid => {
          val key =(cid,"order")
          map += key -> (map.getOrElse(key,0L)+1L)
        })
      case action if action.pay_category_ids != "null" =>
        val cIds = action.pay_category_ids.split(",")
        cIds.foreach(cId => {
          val key =(cId,"pay")
          map += key -> (map.getOrElse(key,0L)+1L)
        })

      case _ =>


    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other match {
      case o:CategoryAcc =>
        o.map.foreach( {
          case (cidAction,count) =>
            self.map += cidAction -> (self.map.getOrElse(cidAction,0L) + count)
        })
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}
