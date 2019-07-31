package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object ReduceDemo {
  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val tupleDataSet = env.fromCollection(
      List(("java" , 1) , ("java", 1) ,("java" , 1) )
    )

    // 使用reduce执行聚合操作
    // 要将一个DataSet聚合到一起
    val reduceDataSet = tupleDataSet.reduce{
      (t1, t2) =>
        (t2._1, t1._2 + t2._2)
    }

    // 打印测试
    reduceDataSet.print()
  }
}
