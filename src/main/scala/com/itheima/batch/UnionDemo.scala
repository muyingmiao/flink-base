package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object UnionDemo {

  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val textDataSet1 = env.fromCollection(List(
      "hadoop", "hive", "flume"
    ))

    val textDataSet2 = env.fromCollection(List(
      "hadoop", "hive", "spark"
    ))

    textDataSet1.union(textDataSet2).print()
  }
}
