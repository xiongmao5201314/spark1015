package com.atguigu.spark.sql.day01.project


import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

class CityRemarkUdf  extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(Array(StructField("city",StringType)))

  override def bufferSchema: StructType = StructType(Array(StructField("map",MapType(StringType,LongType)),StructField("total",LongType)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      case Row(city_name:String) =>
        buffer(1)=buffer.getLong(1) +1L

       val map= buffer.getMap[String,Long](0)
       buffer(0) = map + (city_name -> (map.getOrElse(city_name,0L)+1L))
      case _ =>
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String,Long](0)
    val map2 = buffer2.getMap[String,Long](0)
    val total1 = buffer1.getLong(1)
    val total2 = buffer2.getLong(1)
    buffer1(0) = map1.foldLeft(map2){
      case (map , (city_name,count)) =>
        map+(city_name-> (map.getOrElse(city_name,0L) +count))
    }
    buffer1(1) = total1 + total2

  }

  override def evaluate(buffer: Row): String = {
    val cityAndCount = buffer.getMap[String,Long](0)
    val total = buffer.getLong(1)
    val list = cityAndCount.toList.sortBy(-_._2).take(2)
    var cityRemarks = list.map {
      case (cityName, count) =>
        CityRemark(cityName, count.toDouble / total)
    }
    cityRemarks :+= CityRemark("其他",cityRemarks.foldLeft(1D)(_-_.cityRadio))
    cityRemarks.mkString(",")
  }
}
case class CityRemark(cityName:String,cityRadio:Double){
  private val f = new DecimalFormat("0.00%")
  override def toString: String = {
    s"$cityName:${f.format(cityRadio.abs)}"
  }
}
