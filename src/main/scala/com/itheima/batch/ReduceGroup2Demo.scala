package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object ReduceGroup2Demo {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tupleDataSet = env.fromCollection(List(
      ("java" , 1) , ("java", 1) ,("scala" , 1)
    ))

    val groupedDataSet = tupleDataSet.groupBy(0)

    // 使用reduceGroup来进行聚合计算
    val resultDataSet = groupedDataSet.reduceGroup{
      iter =>
        iter.reduce{
          (t1, t2) => (t1._1, t1._2 + t2._2)
        }
    }

    resultDataSet.print()
  }
}
