package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object ReduceGroupDemo {
  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val tupleDataSet = env.fromCollection(
      List(("java" , 1) , ("java", 1) ,("scala" , 1) )
    )

    // 分组
    val groupedDataSet: GroupedDataSet[(String, Int)] = tupleDataSet.groupBy(0)
    // 使用reduce来进行聚合计算
    // 会对每一个分组来进行累加
    val reducedDataSet = groupedDataSet.reduce{
      (t1, t2) =>
        // 会将每一个分组中的数据都执行一次reduce
        (t1._1, t1._2 + t2._2)
    }

    reducedDataSet.print()
  }
}
