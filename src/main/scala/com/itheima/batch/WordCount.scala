package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    // 获取Flink批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 构建source
    // 表示从本地内存中的一个集合来构建一个Flink能够处理的数据源
    val textDataSet: DataSet[String] = env.fromCollection(
      List("hadoop hive spark", "flink mapreduce hadoop hive", "flume spark spark hive")
    )

    // 数据处理
    // 1. 将每一个字符串按照空格切分，使用flatMap就可以将数据转换为String，获取到一个一个的单词
    // 先map（必须要将元素转换为一个集合），再flatten
    val wordDataSet: DataSet[String] = textDataSet.flatMap(_.split(" "))

    // 2. 将每一个单词转换为一个元组（单词, 单词的数量）
    // 元素1 -> 元素2，定义一个两个元素的元组，如果多个元素，就需要使用()
    val wordAndCountDataSet: DataSet[(String, Int)] = wordDataSet.map(_ -> 1)

    // 3. 按照单词进行分组
    val groupedDataSet: GroupedDataSet[(String, Int)] = wordAndCountDataSet.groupBy(0)

    // 4. 使用reduce操作或者Flink自带的聚合操作来进行单词的累计（reduce针对分组中的单词进行累加）
    val sumDataSet: AggregateDataSet[(String, Int)] = groupedDataSet.sum(1)

    // 构建sink
    // 5. 打印输出
    // 如果sink是print，不需要添加env.execute()
    sumDataSet.print()
  }
}
