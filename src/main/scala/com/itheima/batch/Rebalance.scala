package com.itheima.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object Rebalance {

  def main(args: Array[String]): Unit = {
    // 构建批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用env.generateSequence创建0-100的并行数据
    val numDataSet: DataSet[Long] = env.generateSequence(0, 100)

    // 使用fiter过滤出来大于8的数字
    val filterDataSet = numDataSet.filter(_ > 8).rebalance()

    // 使用map操作传入RichMapFunction，将当前子任务的ID和数字构建成一个元组
    val tupleDataSet: DataSet[(Long, Long)] = filterDataSet.map(new RichMapFunction[Long, (Long, Long)] {
      override def map(value: Long): (Long, Long) = {
        // 在RichMapFunction中可以使用getRuntimeContext.getIndexOfThisSubtask获取子任务序号
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })

    // 打印测试
    tupleDataSet.print()
  }
}
