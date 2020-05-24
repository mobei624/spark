package com.atguigu.sparkstreaming.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-19 20:36
 */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("UpdateStateByKey")
    val context = new StreamingContext(conf,Seconds(3))
    context.checkpoint("./ck")

    val stream: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)

    //updateStateByKey  只对k,v型数据使用
    //seq ：一个批次中，同一个k的所有值组成的序列  opt : 上一个批次中，对k聚合的结果
    val value: DStream[(String, Int)] = stream.flatMap(_.split(" "))
        .map((_, 1))
        .updateStateByKey((seq,opt) =>{
        Some(seq.sum + opt.getOrElse(0))
        })

    value.print

    context.start()
    context.awaitTermination()
  }

}
