package com.atguigu.sparkcore.project.acc

import com.atguigu.sparkcore.project.bean.UserAction
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
 * @author mobei
 * @create 2020-05-11 14:22
 */
class CategoryAcc extends AccumulatorV2[UserAction, mutable.Map[String, (Long, Long, Long)]] {

  private val map = mutable.Map[String, (Long, Long, Long)]()

  //判断集合是否为空
  override def isZero: Boolean = map.isEmpty

  //复制累加器
  override def copy (): AccumulatorV2[UserAction, mutable.Map[String, (Long, Long, Long)]] = {
    val acc = new CategoryAcc
    acc.synchronized{
      acc.map ++= map
    }

    acc
  }

  //重置累加器
  override def reset (): Unit = map.clear()

  //分区内累加
  override def add (v: UserAction) = {
    //对进来的用户行为判断分类，搜索、点击、下单、支付
    //进来一次数据，对数据进行一次累加
    v match {
      //点击
      case action if action.click_category_id != -1 =>
        val cid = action.click_category_id.toString
        val (click, order, pay) = map.getOrElse(cid, (0L, 0L, 0L))
        map += cid -> (click + 1, order, pay)

      //下单
      case action if action.order_category_ids != "null" =>
        val cids = action.order_category_ids.split(",")
        cids.foreach(
          cid =>{
        val (click, order, pay) = map.getOrElse(cid, (0L, 0L, 0L))
        map += cid -> (click, order + 1, pay)}
    )

      //支付
      case action if action.pay_category_ids != "null" =>
        val cids = action.pay_category_ids.split(",")
        cids.foreach(
          cid =>{
            val (click, order, pay) = map.getOrElse(cid, (0L, 0L, 0L))
            map += cid -> (click, order, pay + 1)
          }
    )

      //其他行为不处理
      case _ =>
    }

  }

  //分区间合并
  override def merge (other: AccumulatorV2[UserAction, mutable.Map[String, (Long, Long, Long)]] ): Unit = {
    other match {
      case o:CategoryAcc =>
        o.map.foldLeft(map){
          case (map, (cid, (click, order, pay))) =>
            val (thisClick, thisOrder, thisPay) = map.getOrElse(cid,(0L, 0L, 0L))
            map += cid->(thisClick + click, thisOrder + order, thisPay + pay)
            map
        }
      case _ =>
    }
  }

  //返回值
  override def value: mutable.Map[String, (Long, Long, Long)] = map
}
