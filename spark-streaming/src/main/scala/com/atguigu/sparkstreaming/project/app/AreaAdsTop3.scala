package com.atguigu.sparkstreaming.project.app

import com.atguigu.sparkstreaming.project.bean.AdsInfo
import com.atguigu.sparkstreaming.project.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author mobei
 * @create 2020-05-18 21:22
 */
object AreaAdsTop3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("AreaAdsTop3")
    val context = new StreamingContext(conf, Seconds(3))
    context.checkpoint("./ck")

    val stream = MyKafkaUtil.getDStream(context, "ads_log")

    val adsInfoStream = stream.map {
      case log =>
        val splits = log.split(",")
        AdsInfo(splits(0).toLong,
          splits(1),
          splits(2),
          splits(3),
          splits(4))
    }

    // 需求分析:
    /*

        DStream[(day, area, ads), 1]  updateStateByKey
        DStream[(day, area, ads), count]

        分组, top 3
        DStream[(day, area), (ads, count)]
        DStream[(day, area), it[(ads, count)]]
        排序取前3
     */

    val adsClickTop3 = adsInfoStream
      .map(info => ((info.dayString, info.area, info.adsId), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .map {
        case ((day, area, ads), count) =>
          ((day, area), (ads, count))
      }
      .groupByKey()
      .mapValues {
        it => it.toList.sortBy(-_._2).take(3)
      }
    adsClickTop3.print

    context.start()
    context.awaitTermination()
  }

}
