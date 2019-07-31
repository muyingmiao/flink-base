package com.itheima.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WordCount {
  def main(args: Array[String]): Unit = {
    // 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 构建socket流数据源，并指定IP地址和端口号
    val socketDataStream: DataStream[String] = env.socketTextStream("cdh1", 9999)

    // 对接收到的数据转换成单词元组
    // <hadoop, 1>, <spark,1>
    val wordAndCountDataStream = socketDataStream.flatMap(_.split(" "))
      .map(_ -> 1)

    // 使用keyBy进行分流（分组）
    // 如果使用的是索引值来进行分组，会将分组的字段封装在一个Tuple里面
    // 因为分组字段可以是多个
    val groupedDataStream: KeyedStream[(String, Int), String] = wordAndCountDataStream.keyBy(_._1)

    // 使用timeWinodw指定窗口的长度（每5秒计算一次）
    val windowedDataStream =
      groupedDataStream.countWindow(5L, 1L)

    // 使用sum执行累加
    val reducedDataStream: DataStream[(String, Int)] = windowedDataStream.reduce {
      (t1, t2) =>
        (t1._1, t1._2 + t2._2)
    }

    // 打印输出
    reducedDataStream.print()

    // 启动执行
    env.execute("App")

    // 在Linux中，使用nc -lk 端口号监听端口，并发送单词
  }
}
