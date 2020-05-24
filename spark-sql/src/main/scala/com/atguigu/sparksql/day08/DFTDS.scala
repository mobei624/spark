package com.atguigu.sparksql.day08

import org.apache.spark.sql.SparkSession

/**
 * @author mobei
 * @create 2020-05-14 21:52
 */
object DFTDS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
          .master("local[2]")
          .appName("")
          .getOrCreate()

        import spark.implicits._

    val list = User1("zs", 16, "male") :: User1("wx", 11, "female") :: User1("zq", 32, "male") :: Nil
    val df = list.toDF
    //DF-->DS，有样例类
    val ds = df.as[User1]
    val users = ds.takeAsList(1)
    println(users)

    //DS-->DF，直接toDF
    val ds1 = ds.toDF

    spark.close
  }

}
