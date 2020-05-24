package com.atguigu.sparkcore.project.app

import com.atguigu.sparkcore.project.acc.CategoryAcc
import com.atguigu.sparkcore.project.bean.{CategoryCount, UserAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author mobei
 * @create 2020-05-11 14:15
 *        定义累加器，用累加器计算点击量、下单量、支付量
 */
object CategoryTopApp {

  def caclCategoryTopN(context: SparkContext, actionRDD:RDD[UserAction]) ={

    // 1. 创建累加器对象
    val acc: CategoryAcc = new CategoryAcc
    // 2. 注册累加器
    context.register(acc,"CategoryAcc")
    // 3. 遍历RDD, 进行累加
    actionRDD.foreach(action => acc.add(action))

    // 4. 进数据进行处理   top10
    val map: mutable.Map[String, (Long, Long, Long)] = acc.value

    val result: List[CategoryCount] = map.map {
      case (cid, (click, order, pay)) => CategoryCount(cid, click, order, pay)
    }
      .toList
      .sortBy(x => (-x.click, -x.order, -x.pay))
      .take(10)

    println(result)
    //把result返回，给需求2使用
    result
  }


}
