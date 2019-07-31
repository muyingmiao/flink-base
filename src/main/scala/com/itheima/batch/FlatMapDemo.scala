package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    // 构建批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 构建本地集合数据源
    val textDataSet: DataSet[String] = env.fromCollection(
      List(
        "张三,中国,江西省,南昌市",
        "李四,中国,河北省,石家庄市",
        "Tom,America,NewYork,Manhattan"
      )
    )

    // 使用flatMap将一条数据转换为三条数据
    val userAddressDataSet = textDataSet.flatMap{
      text =>
        val fieldArr = text.split(",")
        // 使用逗号分隔字段
        // 分别构建国家、国家省份、国家省份城市三个元组
        List(
          (fieldArr(0), fieldArr(1)), // 国家维度
          (fieldArr(0), fieldArr(1) + fieldArr(2)), // 省份维度
          (fieldArr(0), fieldArr(1) + fieldArr(2) + fieldArr(3))// 城市维度
        )
    }

    // 打印输出
    userAddressDataSet.print()
  }
}
