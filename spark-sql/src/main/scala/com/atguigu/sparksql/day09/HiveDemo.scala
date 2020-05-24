package com.atguigu.sparksql.day09

import org.apache.spark.sql.SparkSession

/**
 * @author mobei
 * @create 2020-05-15 17:26
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("HiveDemo")
      //支持hive
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

//    spark.sql("show databases").show
    spark.sql("use gmall")
    spark.sql("select * from dws_coupon_use_daycount").show

    spark.close
  }

}
