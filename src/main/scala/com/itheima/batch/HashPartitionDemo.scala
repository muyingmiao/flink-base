package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object HashPartitionDemo {

  def main(args: Array[String]): Unit = {
    // 构建批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 设置并行度为2
    env.setParallelism(2)

    // 使用fromCollection构建测试数据集
    val numDataSet = env.fromCollection(
      List(1,1,1,1,1,1,1,2,2,2,2,2)
    )

    // 使用partitionByHash按照字符串的hash进行分区
    val partitionDataSet: DataSet[Int] = numDataSet.partitionByHash(_.toString)

    // 调用writeAsText写入文件到data/parition_output目录中
    partitionDataSet.writeAsText("./data/parition_output")

    // 打印测试
    partitionDataSet.print()
  }
}
