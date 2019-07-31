package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object SourceDemo {

  case class Subject(id:Int, name:String)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 读取本地文件成DataSet，并进行打印
//    val textFileDataSet = env.readTextFile("./data/input/")
//
//    textFileDataSet.print()

    // 读取HDFS文件成为一个DataSet，进行打印
//    val hdfsFileDataSet = env.readTextFile("hdfs://cdh1:8020/wordcount.txt")
//    hdfsFileDataSet.print()

//    val subjectDataSet: DataSet[Subject] = env.readCsvFile[Subject]("data/input/subject.csv")
//    subjectDataSet.print()

    // 使用readTextFile读取gz压缩文件
    val textDataSet: DataSet[String] = env.readTextFile("data/input/wordcount.txt.gz")
    textDataSet.print()
  }
}
