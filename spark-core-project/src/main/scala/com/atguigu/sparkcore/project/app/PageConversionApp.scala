package com.atguigu.sparkcore.project.app

import java.text.DecimalFormat

import com.atguigu.sparkcore.project.bean.{CategoryCount, UserAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author mobei
 * @create 2020-05-12 13:49
 *        需求3：页面单跳转转化率
 *        即访问到A页面，然后从A页面跳转到B页面的转化率
 */
object PageConversionApp {

  def calcPageConversion(context: SparkContext,
                         userActionRDD:RDD[UserAction],
                         targetPageFlow: String): Unit ={

    // 1. 目标跳转页面切开
    val pages: Array[String] = targetPageFlow.split(",")
    // 1.1 目标跳转流 (跳转起始页面，跳转目标页面, 起始页->目标页 的集合)
    val prePages: Array[String] = pages.dropRight(1)
    val desPages: Array[String] = pages.tail

    val targetFlows = prePages
      .zip(desPages)
      .map(flows => flows._1 + "->" + flows._2)

    // 2. 计算分母  prePages中所有页面的访问量，即访问到这个页面的总数
    val pageCount = userActionRDD
      .filter(action => prePages.contains(action.page_id.toString) //过滤出行动日志中，跳转起始页面包含目标页面的记录
      )
      .map(action => (action.page_id, 1)) //有此条记录，页面id记1
//      .reduceByKey(_ + _)                 //计算每个页面访问的总数
      .countByKey()


    // 3. 计算分子
    // 3.1 按照sessionId分组, 每个组内再去计算他们的跳转量
    val pageFlowsRDD = userActionRDD
      .groupBy(_.session_id)   //按照session分组，里面包括这个session下所有的操作
      .flatMap{
        //需要获取1->2, 2->3, 3->4  ....这样的key对应的个数
        case (sid,actionIt) =>
          //按操作的时间戳升序排序
          val actions = actionIt.toList.sortBy(_.action_time)
          //调整跳转流
          val preActions = actions.init      //跳转页
          val desActions = actions.tail      //目标页

          //获取每个session里面所有的跳转流
          val allFlows = preActions.zip(desActions).map({
            case (pre, post) => s"${pre.page_id}->${post.page_id}"
          })

          //过滤出目标跳转流
          val desFlows = allFlows.filter{
            case allFlows => targetFlows.contains(allFlows)
          }

          desFlows
      }
    // 3.2 聚合跳转流
    val pageFlowCount = pageFlowsRDD.map((_, 1)).reduceByKey(_ + _)

    //计算转化率
    val format = new DecimalFormat(".00%")

    val rate = pageFlowCount.map {

      case (flow, count) =>
        val page = (flow.split("->")(0), count)

//        println(pageCount(page._1.toLong))
        (flow, format.format(page._2.toDouble / pageCount(page._1.toLong)))

    }

    rate.collect.foreach(println)

  }

}
