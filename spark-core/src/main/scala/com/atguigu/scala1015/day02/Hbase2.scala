package com.atguigu.scala1015.day02
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Serialization

import scala.collection.mutable

object Hbase2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hbase2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop104,hadoop106,hadoop108")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student")

    val rdd1 = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val resultRDD = rdd1.map {
      case (iw, result) =>
      val map = mutable.Map[String, Any]()
     map += "rowkey" -> Bytes.toString(iw.get())
        val cells = result.listCells()
        import  scala.collection.JavaConversions._
        for (cell <- cells){
          val key = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          map += key -> value
        }
        implicit  val df = org.json4s.DefaultFormats
        Serialization.write(map)
    }
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
