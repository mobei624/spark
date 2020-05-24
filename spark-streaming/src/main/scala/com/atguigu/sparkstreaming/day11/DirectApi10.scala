package com.atguigu.sparkstreaming.day11

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-18 17:11
 *
 *        kafka直联模式
 *        kafka-0-10_2.11
 */
object DirectApi10 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("DirectApi10")
    val context = new StreamingContext(conf,Seconds(3))

    val topics = Array("spark1128")
      val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "key.deserializer" -> classOf[StringDeserializer],  // key的反序列化器
        "value.deserializer" -> classOf[StringDeserializer], // value的反序列化器
        "group.id" -> "atguigu1",
        "auto.offset.reset" -> "latest",  // 每次从最新的位置开始读
        "enable.auto.commit" -> (true: java.lang.Boolean)  // 自动提交kafka的offset
      )

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      context,
      locationStrategy = LocationStrategies.PreferConsistent, // 平均分布
      Subscribe[String, String](topics, kafkaParams)
    )

    val value: DStream[String] = stream.map(_.value())
    value.print

    context.start()
    context.awaitTermination()
  }

}
