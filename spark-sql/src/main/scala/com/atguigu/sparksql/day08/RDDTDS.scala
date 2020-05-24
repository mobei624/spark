package com.atguigu.sparksql.day08

import org.apache.spark.sql.SparkSession

/**
 * @author mobei
 * @create 2020-05-14 21:31
 */
object RDDTDS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
          .master("local[2]")
          .appName("")
          .getOrCreate()

        import spark.implicits._

    /*val list = List("aaa", "bac", "ade", "efg")
    val ds = list.toDS
    ds.show*/

    val list = User1("zs", 16, "male") :: User1("wx", 11, "female") :: User1("zq", 32, "male") :: Nil
    val ds = list.toDS
    ds.show

    ds.filter(_.age > 12).show

    val rdd1 = ds.rdd
    rdd1.collect.foreach(println)
    spark.close
  }

}

case class User1(name:String, age:Int, gender:String)