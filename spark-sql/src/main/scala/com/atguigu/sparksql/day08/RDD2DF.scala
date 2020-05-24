package com.atguigu.sparksql.day08

import org.apache.spark.sql.SparkSession


/**
 * @author mobei
 * @create 2020-05-14 20:08
 *
 *        RDD转DF
 */
object RDD2DF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DS")
      .getOrCreate()

    import spark.implicits._

    val list = List((30,"a"), (50,"b"), (70,"c"), (80,"d"))
    val rdd1 = spark.sparkContext.parallelize(list)
    val value = rdd1.toDF("name","age")

    value.show()
    val rdd2 = value.rdd

    //row看成一个集合, 有下标，下标即对应的列 0,1,2...
    val value1 = rdd2.map(row => (row.getInt(0),row.getString(1)))

    value1.collect.foreach(println)

    /*val list = User("zs", 16) :: User("wx", 11) :: User("zq", 32) :: Nil
    val rdd1 = spark.sparkContext.parallelize(list)
    val df1 = rdd1.toDF   //如果rdd中是样例类，转成df时不需要加列名，会自动获取样例类的属性名作为列名
    df1.show*/


    spark.close()

  }

}

case class User(name:String, age:Int)