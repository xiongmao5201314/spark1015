package com.atguigu.scala1015.day02

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Jdbc2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Jdbc2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val url ="jdbc:mysql://hadoop106:3306/gmall"
    val user = "root"
    val password = "123321"
    val rdd = sc.parallelize((1004, "zs", 106) :: (1002, "dd", 105) :: (1003, "dd", 104) :: Nil)
    Class.forName("com.mysql.jdbc.Driver")
//    val conn = DriverManager.getConnection(url, user, password)
    val sql ="insert into test values(?,?,?)"
    rdd.foreachPartition(it => {
      val conn = DriverManager.getConnection(url, user, password)
      val ps = conn.prepareStatement(sql)
      var count =0
      it.foreach{
        case (id,name,id2) => {

          ps.setInt(1,id)
          ps.setString(2,name)
          ps.setInt(3,id2)
          ps.addBatch()
          count += 1
          if (count % 100 == 0 ) {
                ps.executeBatch()
                count = 0
          }
        }


      }
      ps.executeBatch()
      conn.close()
    })
    /*rdd.foreachPartition(it => {
      val conn = DriverManager.getConnection(url, user, password)

      it.foreach{
        case (id,name,id2) => {
        val ps = conn.prepareStatement(sql)
        ps.setInt(1,id)
        ps.setString(2,name)
        ps.setInt(3,id2)
        ps.execute()
        ps.close()
        }
      }
      conn.close()
    })*/
    /*rdd.foreach{
      case(id,name,id2) => {
        val conn = DriverManager.getConnection(url, user, password)
        val ps = conn.prepareStatement(sql)
        ps.setInt(1,id)
        ps.setString(2,name)
        ps.setInt(3,id2)
        ps.execute()
        ps.close()
        conn.close()
      }

    }*/


    sc.stop()






    /*    val rdd2 = new JdbcRDD(
      sc,
      () => {
        //加载驱动
        Class.forName("com.mysql.jdbc.Driver")
        DriverManager.getConnection(url, user, password)
      },
      "select * from base_category3 where id >= ? and id <= ?",
      1,
      10,
      2,
      resultSet => (resultSet.getInt(1),resultSet.getString(2))
    )
    rdd2.collect().foreach(println)*/
  }


}
