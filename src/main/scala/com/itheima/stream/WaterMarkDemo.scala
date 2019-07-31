package com.itheima.stream

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object WaterMarkDemo {

  // 创建一个订单样例类Order，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(id:String, userId:Int, money:Long, createTime:Long)

  def main(args: Array[String]): Unit = {    //创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置处理时间为EventTime
    // 事件时间、摄入时间、处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建一个自定义数据源
    // 添加一个添加自动生成订单数据的source
    val orderDataStream: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      // 记录当前是否继续需要生成数据
      var isRunning = true

      // 使用一个变量控制停止生成数据
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        while (isRunning) {

          // 随机生成订单ID（UUID）
          // 随机生成用户ID（0-2）
          // 随机生成订单金额（0-100）
          // 时间戳为当前系统时间
          // 每隔1秒生成一个订单
          // Random.nextInt表示随机生成用户ID, 0\1\2
          val order = Order(UUID.randomUUID().toString, Random.nextInt(3), Random.nextInt(101), System.currentTimeMillis())
          ctx.collect(order)
          // 每秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // 添加水印
    val watermarkDataStream: DataStream[Order] = orderDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order] {
      var currentTimestamp: Long = _
      // 允许延迟2秒
      val delayTime = 2000

      // 生成一个水印数据
      override def getCurrentWatermark: Watermark = {
        // 减去两秒中，表示让window窗口延迟两秒计算
        // 10:05 - 2s
        val watermark = new Watermark(currentTimestamp - delayTime)
        // 在获取水印方法中，打印水印时间、事件时间和当前系统时间
        // scala的插值表达式，避免使用+来拼接字段串
        // 日期格式化
        // FastDateFormat 12319823891
        val formater = FastDateFormat.getInstance("HH:mm:ss")

        println(s"水印时间: ${formater.format(watermark.getTimestamp)}，事件时间：${formater.format(currentTimestamp)}, 系统时间：${formater.format(System.currentTimeMillis())}")
        watermark
      }

      // 表示从Order中获取对应的时间戳
      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        // 获取到Order订单事件的时间戳
        val timestamp = element.createTime
        // 表示时间轴不会往前推，不能因为某些数据延迟了，导致整个window数据得不到计算
        // Order：10:05:00 - 10:10
        // currentTime Order: 10:09:58
        currentTimestamp = Math.max(currentTimestamp, timestamp)
        currentTimestamp
      }
    })

    // 使用keyBy按照用户名分流
    val groupedDataStream: KeyedStream[Order, Int] = watermarkDataStream.keyBy(_.userId)

    // 使用timeWindow划分时间窗口
    // 10:00:00 - 10:00:05 - 10:00:10 - 10:00:15
    val windowedDataStream = groupedDataStream.timeWindow(Time.seconds(5))


    val totalMoneyDataStream: DataStream[Long] =
    windowedDataStream.apply(new RichWindowFunction[Order, Long, Int, TimeWindow] {
      var totalMoney: Long = _

      // 使用apply进行自定义计算
      // 在apply方法中实现累加
      override def apply(key: Int, window: TimeWindow, input: Iterable[Order], out: Collector[Long]): Unit = {
        // 用户的订单总金额计算
        for (elem <- input) {
          totalMoney = totalMoney + elem.money
        }
        out.collect(totalMoney)
      }
    })

    // 运行测试
    totalMoneyDataStream.print()

    //查看HDFS中是否已经生成checkpoint数据
    env.execute("App")
  }
}
