package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object MapPartitionDemo {
  // 创建一个User样例类
  case class User(id:Int, name:String)

  def main(args: Array[String]): Unit = {
    // 获取ExecutionEnvironment运行环境
    // 使用fromCollection构建数据源
    // 获取ExecutionEnvironment运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 使用fromCollection构建数据源
    val textDataSet = env.fromCollection(List(
      "1,张三", "2,李四", "3,王五", "4,赵六"
    ))

    // 使用mapPartition操作执行转换
    val userDataSet = textDataSet.mapPartition{
      iter =>
        // TODO:打开redis/mysql连接
        iter.map {
          elem =>
            val fieldArr = elem.split(",")
            User(fieldArr(0).toInt, fieldArr(1))
        }
        // TODO：关闭redis/mysql连接
    }

    // 打印测试
    userDataSet.print()
  }
}
