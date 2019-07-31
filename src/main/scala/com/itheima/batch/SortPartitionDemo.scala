package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object SortPartitionDemo {

  def main(args: Array[String]): Unit = {
    // 构建批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建测试数据集
    val wordDataSet = env.fromCollection(
      List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink")
      // 设置数据集的并行度为2
    ).setParallelism(1)

    // 使用sortPartition按照字符串进行降序排序
    val sortedDataSet: DataSet[String] = wordDataSet.sortPartition(_.toString, org.apache.flink.api.common.operators.Order.DESCENDING)


    // 调用writeAsText写入文件到data/sort_output目录中
    sortedDataSet.writeAsText("./data/sort_output")

    // 启动执行
    env.execute("SortApp")
  }
}
