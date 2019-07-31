package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FilterDemo {
  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val wordDataSet = env.fromCollection(List(
      "hadoop", "hive", "spark", "flink"
    ))

    // 使用filter操作执行过滤
    // 过滤出来以h开头的单词
    val resultDataSet = wordDataSet.filter(_.startsWith("h"))

    // 打印测试
    resultDataSet.print()
  }
}
