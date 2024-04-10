package com.atguigu.spark.sql.day01

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object UDF1 {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("UDF1").master("local[2]").getOrCreate()
    import  spark.implicits._
    val df = spark.read.json("d:/json.txt")
    df.createOrReplaceTempView("user")
    spark.udf.register("myAvg",new myAvg)
    spark.sql("select myAvg(salary) from user").show()
    spark.close()
  }
}

class mySum extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}
class myAvg extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
    buffer(1) =0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer.getDouble(0) /buffer.getLong(1)
}
