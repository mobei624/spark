package com.atguigu.scala.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author mobei
 * @create 2020-05-07 23:36
 */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AggregateByKey").setMaster("local[2]")
    val context = new SparkContext(conf)
    val list1 = List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8))
    val rdd1 = context.parallelize(list1)

   // val rdd2 = rdd1.aggregateByKey(Int.MinValue)(Math.max(_, _), _ + _)

    /*//分区内求最大值和最小值，分区间求最大值的和和最小值的和
    val rdd2 = rdd1.aggregateByKey((Int.MinValue, Int.MaxValue))(
      {
        case ((max, min), value) => (max.max(value), min.min(value))
      },
      {
        case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
      })*/

    //求平均值
    //分区内求和及个数，分区间和相加，个数相加
    val rdd2 = rdd1.aggregateByKey((0, 0))(
      {
        case ((sum, count), value) => (sum + value, count + 1)
      },
      {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }
    )
      .mapValues({
      case (sum, count) => sum.toDouble / count
    })
//        .mapValues(value => value._1.toDouble / value._2)
    rdd2.collect.foreach(println)
    context.stop()
  }

}
