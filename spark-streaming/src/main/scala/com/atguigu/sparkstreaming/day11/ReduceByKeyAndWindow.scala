package com.atguigu.sparkstreaming.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-19 21:07
 *        ReduceByKeyAndWindow : 计算多个批次的聚合结果
 *
 */
object ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("ReduceByKeyAndWindow")
    val context = new StreamingContext(conf,Seconds(3))

    val stream = context.socketTextStream("hadoop102", 9999)
    val value = stream.flatMap(_.split(" "))
      .map((_, 1))
      //窗口大小和滑动步长 需是批次时间的整数倍
      // windowDuration 窗口大小
      //slideDuration 滑动步长，如果不给，则默认和批次时间一致
//      .reduceByKeyAndWindow(_ + _, windowDuration = Seconds(9), slideDuration = Seconds(6))
    //如果滑动时窗口有重叠，则可以加另外的参数优化
      //(now, pre) => now - pre   now:进来的窗口d  pre: 离开的窗口a
      // 例如原来的窗口是三个批次，滑动步长是1  窗口1：（a, b, c） 窗口1：（b, c, d）
      // 重叠部分的（b,c）的值在上一个窗口已经算出来了，由spark保存，则新的窗口里面不需要重新计算重叠批次
        .reduceByKeyAndWindow(_ + _, (now, pre) => now - pre, Seconds(9))


    value.print

    context.start()
    context.awaitTermination()
  }

}
