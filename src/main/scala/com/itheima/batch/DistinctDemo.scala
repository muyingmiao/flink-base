package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object DistinctDemo {

  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val tupleDataSet = env.fromCollection(
      List(("java" , 1) , ("java", 1) ,("scala" , 1) )
    )

    // distinct是可以指定字段来进行去重
    tupleDataSet.distinct(0).print()
  }
}
