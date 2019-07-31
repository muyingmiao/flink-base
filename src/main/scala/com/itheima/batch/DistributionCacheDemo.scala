package com.itheima.batch

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.io.Source

object DistributionCacheDemo {

  def main(args: Array[String]): Unit = {
    //获取批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //创建成绩数据集
    val scoreDataSet = env.fromCollection(List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86)))

    // 注册了一个分布式的缓存
    env.registerCachedFile("hdfs://cdh1:8020/distribute_cache_student", "student")

    //对成绩数据集进行map转换，将（学生ID, 学科, 分数）转换为（学生姓名，学科，分数）
    val resultDataSet = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      var list: List[(Int, String)] = _

      override def open(parameters: Configuration): Unit = {
        //使用getRuntimeContext.getDistributedCache.getFile获取分布式缓存文件
        val file: File = getRuntimeContext.getDistributedCache.getFile("student")
        //使用Scala.fromFile读取文件，并获取行
        val lineIter: Iterator[String] = Source.fromFile(file).getLines()
        //将文本转换为元组（学生ID，学生姓名），再转换为List
        list = lineIter.map{
          line =>
            val fieldArr = line.split(",")
            (fieldArr(0).toInt, fieldArr(1))
        }.toList
      }

      //  在map方法中使用广播进行转换
      //打印测试
      override def map(value: (Int, String, Int)): (String, String, Int) = {
        // 获取学生的ID
        //获取学生姓名,构建最终结果元组
        val studentId = value._1
        val studentName = list.filter(_._1 == studentId)(0)._2

        //将成绩数据(学生ID，学科，成绩) -> (学生姓名，学科，成绩)
        (studentName, value._2, value._3)
      }
    })

    resultDataSet.print()
  }
}
