package com.atguigu.sparkcore.project.app

import com.atguigu.sparkcore.project.bean.{CategoryCount, UserAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author mobei
 * @create 2020-05-11 21:19
 *
 * 需求 2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
 *      获取需求1 的categoryid（10个），每一个都获取对它点击次数排名前10的sessionid
 */
object CategorySessionTopApp {

  //解法1   缺点：在排序时要将迭代器转成集合容器，容易oom
  def caclCategoryTopNSession(context: SparkContext,
                              categoryCountList:List[CategoryCount],
                              userActionRDD:RDD[UserAction]) ={

    // 1. 过滤出来 top10品类的所有点击记录
    val cids = categoryCountList.map(_.cid.toLong)

    val topCategoryActionRDD: RDD[UserAction] = userActionRDD.filter(action => cids.contains(action.click_category_id))
    // 2. 计算每个品类 下的每个session 的点击量总和
    // 每个品类每个session记1 rdd ((cid, sid) ,1)
    // 改变数据格式 rdd ((cid, sid) ,count)  ->  (cid, (sid ,count))
    val countRDD = topCategoryActionRDD
      .map(action => ((action.click_category_id, action.session_id), 1))
      .reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }

    //3. 按照品类分组
    // 4. 排序, 取top10
    val result = countRDD
      .groupByKey()
      .map{
        case (cid, groupIt) => (cid, groupIt.toList.sortBy(-_._2).take(10))
      }

    result.collect.foreach(println)

  }

  //解法2
  def caclCategoryTopNSession1(context: SparkContext,
                              categoryCountList:List[CategoryCount],
                              userActionRDD:RDD[UserAction]) ={

    // 1. 过滤出来 top10品类的所有点击记录
    val cids = categoryCountList.map(_.cid.toLong)

    val topCategoryActionRDD: RDD[UserAction] = userActionRDD.filter(action => cids.contains(action.click_category_id))
    // 2. 计算每个品类 下的每个session 的点击量总和
    // 每个品类每个session记1 rdd ((cid, sid) ,1)
    // 改变数据格式 rdd ((cid, sid) ,count)  ->  (cid, (sid ,count))
    val countRDD = topCategoryActionRDD
      .map(action => ((action.click_category_id, action.session_id), 1))
      .reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }



//    result.collect.foreach(println)

  }
}
