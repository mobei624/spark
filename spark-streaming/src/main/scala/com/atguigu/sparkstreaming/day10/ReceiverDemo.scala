package com.atguigu.sparkstreaming.day10

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


/**
 * @author mobei
 * @create 2020-05-17 22:12
 */
object ReceiverDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("")
    val context = new StreamingContext(conf,Seconds(3))

    val stream = context.receiverStream(new MyReceiver("hadoop102", 10000))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    stream.print


    context.start()
    context.awaitTermination()
  }

}

class MyReceiver(host: String, port: Int) extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY){

  /**
   * 用来真正的去读数据
   * 不能阻塞, 所以, 读数据的代码, 应该在一个单独的子线程中
   */
  var socket: Socket = _
  var reader: BufferedReader = _

  override def onStart(): Unit = {
    // 防止阻塞onStart, 所以把代码在子线程中执行
    runInThread {
      try {
        // 从socket读数据
        socket = new Socket(host, port)
        reader =
          new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
        var line = reader.readLine()
        // 表示读到了数据
        while (line != null) {
          store(line) // 将来spark'会分发其他的executor进行处理
          line = reader.readLine() // 如果没有数据, 这里会阻塞, 等待数据的输入
        }
      } catch {
        case e => println(e.getMessage)
      } finally {
        restart("重启接收器") // 先回调onStop, 再回调 onStart
      }
    }
  }

  /**
   * 用来释放资源
   */
  override def onStop(): Unit = {
    if (reader != null) reader.close()
    if (socket != null) socket.close()
  }

  // 把传入的代码运行在子线程
  def runInThread(op: => Unit) = {
    new Thread() {
      override def run(): Unit = op
    }.start()
  }
}
