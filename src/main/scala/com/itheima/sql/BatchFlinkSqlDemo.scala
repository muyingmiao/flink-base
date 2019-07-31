package com.itheima.sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object BatchFlinkSqlDemo {

  // 创建一个样例类Order用来映射数据（订单名、用户名、订单日期、订单金额）
  case class Order(id:Int, userName:String, createTime:String, money:Double)

  def main(args: Array[String]): Unit = {
    // 获取一个批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 获取一个Table运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 基于本地Order集合创建一个DataSet source
    val orderDataSet = env.fromCollection(List(
      Order(1, "zhangsan", "2018-10-20 15:30", 358.5),
      Order(2, "zhangsan", "2018-10-20 16:30", 131.5),
      Order(3, "lisi", "2018-10-20 16:30", 127.5),
      Order(4, "lisi", "2018-10-20 16:30", 328.5),
      Order(5, "lisi", "2018-10-20 16:30", 432.5),
      Order(6, "zhaoliu", "2018-10-20 22:30", 451.0),
      Order(7, "zhaoliu", "2018-10-20 22:30", 362.0),
      Order(8, "zhaoliu", "2018-10-20 22:30", 364.0),
      Order(9, "zhaoliu", "2018-10-20 22:30", 341.0)
    ))

    // 使用Table运行环境将DataSet注册为一张表
    tableEnv.registerDataSet("t_order", orderDataSet)

    // 使用SQL语句来操作数据（统计用户消费订单的总金额、最大金额、最小金额、订单总数）
    val sql =
      """
        |select
        | userName,
        | sum(money) as totalMoney,
        | max(money) as maxMoney,
        | min(money) as minMoney,
        | count(1) as totalCount
        |from
        | t_order
        |group by
        | userName
      """.stripMargin

    val table: Table = tableEnv.sqlQuery(sql)
    // 打印输出的这个表格的定义（哪些列、列的类型）
    table.printSchema()

    // 使用TableEnv.toDataSet将Table转换为DataSet
    val resultDataSet: DataSet[Row] = tableEnv.toDataSet[Row](table)

    // 打印测试
    resultDataSet.print()
  }
}
