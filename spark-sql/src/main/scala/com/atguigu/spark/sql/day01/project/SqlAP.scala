package com.atguigu.spark.sql.day01.project

import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.tools.cmd.Property

object SqlAP {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SqlAP")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.udf.register("remark",new CityRemarkUdf)
//    spark.sql("show databases").show

    spark.sql("use spark1016")
//    spark.sql("select * from city_info").show()
    spark.sql("""select
                |  ci.*,
                |  pi.product_name,
                |  uva.click_product_id
                |
                |from user_visit_action uva
                |join
                |product_info pi on uva.click_product_id = pi.product_id
                |join
                |city_info ci on uva.city_id = ci.city_id""".stripMargin).createOrReplaceTempView("t1")

    spark.sql("""select
                |    area,
                |    product_name,
                |    count(*) count,
                |    remark(city_name) remark
                |from t1
                |group by area,product_name""".stripMargin).createOrReplaceTempView("t2")

    spark.sql("""select
                |    area,
                |    product_name,
                |    count,
                |    remark,
                |    rank() over(partition by area order by count desc) rk
                |from t2""".stripMargin).createOrReplaceTempView("t3")
      val props = new Properties()
    props.put("user","root")
    props.put("password","123321")
    spark.sql("""select
                |    area,
                |    product_name,
                |    count,
                |    remark
                |from t3
                |where rk<=3""".stripMargin)
      .coalesce(1)
        .write
      .mode("overwrite")
      .jdbc("jdbc:mysql://hadoop106:3306/gmall?useUnicode=true&characterEncoding=utf8","spark1015",props)
    spark.close()

  }
}

/*
//查出所需字段 t1
select
  ci.*,
  pi.product_name,
  uva.click_product_id

from user_visit_action uva
join
product_info pi on uva.click_product_id = pi.product_id
join
city_info ci on uva.city_id = ci.city_id

//按照地区和商品名称聚合 t2
select
    area,
    product_name,
    count(*) count
from t1
group by area,product_name

//按照地区分组开窗排序 t3
select
    area,
    product_name,
    count,
    rank() over(partition by area order by count desc) rk
from t2

//按照排序取前3
select
    area,
    product_name,
    count
from t3
where rk <=3



* CREATE TABLE `user_visit_action`(
  `date` string,
  `user_id` bigint,
  `session_id` string,
  `page_id` bigint,
  `action_time` string,
  `search_keyword` string,
  `click_category_id` bigint,
  `click_product_id` bigint,
  `order_category_ids` string,
  `order_product_ids` string,
  `pay_category_ids` string,
  `pay_product_ids` string,
  `city_id` bigint)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/user_visit_action.txt' into table spark1016.user_visit_action;

CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/product_info.txt' into table spark1016.product_info;

CREATE TABLE `city_info`(
  `city_id` bigint,
  `city_name` string,
  `area` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/city_info.txt' into table spark1016.city_info;*/

