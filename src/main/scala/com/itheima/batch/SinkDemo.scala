package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._

object SinkDemo {
  def main(args: Array[String]): Unit = {
    // 将以下单词列表："hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink"，
    // 数据集下沉到本地内存，并打印。

    val env = ExecutionEnvironment.getExecutionEnvironment

    val wordDataSet: DataSet[String] = env.fromCollection(
      List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink")
    )

    // TODO:Transformation

    // 使用collect操作来收集运行的结果到
    // 本地的内存中
    // Seq是scala本地的集合(内存)
//    val localSeq: Seq[String] = wordDataSet.collect()
//    localSeq.foreach(println)

    // 将DataSet的数据写入到本地文件中
//    wordDataSet.writeAsText("./data/output/1.txt")

    wordDataSet.writeAsText("hdfs://cdh1:8020/testData/1.txt")
    env.execute("SinkDemoApp")
  }
}
