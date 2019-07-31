package com.itheima.sql

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.itheima.stream.WaterMarkDemo.Order
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.util.Random

object StreamFlinkSqlDemo {
  // 创建一个订单样例类Order，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(id:String, userId:Int, money:Long, createTime:Long)

  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 获取Table运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 创建一个自定义数据源
    val orderDataStream = env.addSource(new RichSourceFunction[Order] {
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        // 使用for循环生成1000个订单
        for(i <- 0 until 1000) {
          // 随机生成订单ID（UUID）
          val id = UUID.randomUUID().toString
          // 随机生成用户ID（0-2）
          val userId = Random.nextInt(3)
          // 随机生成订单金额（0-100）
          val money = Random.nextInt(101)
          // 时间戳为当前系统时间
          val timestamp = System.currentTimeMillis()

          // 收集数据
          ctx.collect(Order(id, userId, money, timestamp))
          // 每隔1秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = ()
    })

    // 添加水印，允许延迟2秒
    val watermarkDataStream: DataStream[Order] = orderDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order] {
      var currentTimestamp: Long = _
      // 允许延迟2秒
      val delayTime = 2000

      // 生成一个水印数据
      override def getCurrentWatermark: Watermark = {
        // 减去两秒中，表示让window窗口延迟两秒计算
        val watermark = new Watermark(currentTimestamp - delayTime)
        val formater = FastDateFormat.getInstance("HH:mm:ss")

        println(s"水印时间: ${formater.format(watermark.getTimestamp)}，事件时间：${formater.format(currentTimestamp)}, 系统时间：${formater.format(System.currentTimeMillis())}")
        watermark
      }

      // 表示从Order中获取对应的时间戳
      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        // 获取到Order订单事件的时间戳
        val timestamp = element.createTime
        // 表示时间轴不会往前推，不能因为某些数据延迟了，导致整个window数据得不到计算
        currentTimestamp = Math.max(currentTimestamp, timestamp)
        currentTimestamp
      }
    })

    // 使用registerDataStream注册表，并分别指定字段，还要指定rowtime字段
    // 导入import org.apache.flink.table.api.scala._隐式参数
    tableEnv.registerDataStream("t_order", watermarkDataStream, 'id, 'userId, 'money, 'createTime.rowtime)

    // 编写SQL语句统计用户订单总数、最大金额、最小金额
    // 分组时要使用tumble(时间列, interval '窗口时间' second)来创建窗口
    val sql =
      """
        |select
        | userId,
        | count(1) as totalCount,
        | max(money) as maxMoney,
        | min(money) as minMoney
        |from
        | t_order
        |group by
        | tumble(createTime, interval '5' second),
        | userId
      """.stripMargin

    // 使用tableEnv.sqlQuery执行sql语句
    val table: Table = tableEnv.sqlQuery(sql)
    table.printSchema()

    // 将SQL的执行结果转换成DataStream再打印出来
    tableEnv.toAppendStream[Row](table).print()

    // 启动流处理程序
    env.execute("StreamSQLApp")
  }
}
