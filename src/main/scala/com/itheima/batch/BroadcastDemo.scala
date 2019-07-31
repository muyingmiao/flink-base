package com.itheima.batch

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object BroadcastDemo {

  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //分别创建两个数据集
    val studentDataSet = env.fromCollection(List((1, "张三"), (2, "李四"), (3, "王五")))
    val scoreDataSet = env.fromCollection(List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86)))

    //使用RichMapFunction对成绩数据集进行map转换
    val resultDataSet = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      var list: List[(Int, String)] = _

      override def open(parameters: Configuration): Unit = {
        //重写open方法中，获取广播数据
        //导入scala.collection.JavaConverters._隐式转换
        import scala.collection.JavaConverters._

        //将广播数据使用asScala转换为Scala集合，再使用toList转换为scala List集合
        list = getRuntimeContext.getBroadcastVariable[(Int, String)]("student").asScala.toList
      }

      //  在map方法中使用广播进行转换
      //打印测试
      override def map(value: (Int, String, Int)): (String, String, Int) = {
        // 获取学生的ID
        val studentId = value._1
        val studentName = list.filter(_._1 == studentId)(0)._2

        //将成绩数据(学生ID，学科，成绩) -> (学生姓名，学科，成绩)
        (studentName, value._2, value._3)
      }

      //在数据集调用map方法后，调用withBroadcastSet将学生数据集创建广播
    }).withBroadcastSet(studentDataSet, "student")


    resultDataSet.print()
  }
}
