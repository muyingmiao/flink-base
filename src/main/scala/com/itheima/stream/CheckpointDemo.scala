package com.itheima.stream

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * 添加一个source，每秒生成一个订单数据（订单ID、用户ID、订单金额），分别计算出每个用户的订单总金额。
  * 要求使用checkpoint将用户的总金额进行快照。
  */
object CheckpointDemo {

  // 创建一个订单样例类（订单ID、用户ID、订单金额）
  case class Order(id:String, userName:Int, money:Long)

  case class UDFState(totalMoney:Long)

  def main(args: Array[String]): Unit = {
    //创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //配置checkpoint
    // 每1秒开启设置一个checkpoint
    env.enableCheckpointing(1000)
    // 设置checkpoint快照保存到HDFS
    env.setStateBackend(new FsStateBackend("hdfs://cdh1:8020/flink-checkpoint"))
    // 设置checkpoint的执行模式，最多执行一次或者至少执行一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置checkpoint的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //设置同一时间有多少 个checkpoint可以同时执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 添加一个添加自动生成订单数据的source
    val orderDataStream: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      // 记录当前是否继续需要生成数据
      var isRunning = true

      // 使用一个变量控制停止生成数据
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        while (isRunning) {
          // Random.nextInt表示随机生成用户ID, 0\1\2
          val order = Order(UUID.randomUUID().toString, Random.nextInt(3), Random.nextInt(101))
          ctx.collect(order)
          // 每秒生成一个订单
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

//    orderDataStream.print()

    // 使用keyBy按照用户名分流
    val groupedDataStream: KeyedStream[Order, Int] = orderDataStream.keyBy(_.userName)

    // 使用timeWindow划分时间窗口
    val windowedDataStream = groupedDataStream.timeWindow(Time.seconds(5))


    // 创建一个类实现WindowFunction，并实现ListCheckpointed
    // 创建一个UDFState，用户保存checkpoint的数据
    val totalMoneyDataStream: DataStream[Long] =
      windowedDataStream.apply(new RichWindowFunction[Order, Long, Int, TimeWindow] with ListCheckpointed[UDFState] {
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

      // 在snapshotState实现快照存储数据，
      override def snapshotState(checkpointId: Long, timestamp: Long): util.List[UDFState] = {
        // 将当前计算的总金额封装到一个UDF中
        // 因为ListCheckpointed需要将数据保存到HDFS中，所以必须要实现可序列化
        val udfState = UDFState(totalMoney)
        val states = new util.ArrayList[UDFState]()
        states.add(udfState)

        states
      }

      // 在restoreState实现从快照中恢复数据
      override def restoreState(state: util.List[UDFState]): Unit = {
        val udfState = state.get(0)
        totalMoney = udfState.totalMoney
      }
    })

    // 运行测试
    totalMoneyDataStream.print()

    //查看HDFS中是否已经生成checkpoint数据
    env.execute("App")
  }
}
