package com.itheima.batch

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.api.scala._

object AggregateDemo {
  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val tupleDataSet = env.fromCollection(
      List(("java" , 1) , ("java", 1) ,("scala" , 1) )
    )

    // 分组
    val groupedDataSet: GroupedDataSet[(String, Int)] = tupleDataSet.groupBy(0)
    val resultDataSet = groupedDataSet.aggregate(Aggregations.SUM, 1)

    resultDataSet.print()
  }
}
