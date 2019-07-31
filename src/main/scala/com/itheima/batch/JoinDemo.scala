package com.itheima.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object JoinDemo {

  //创建两个样例类
  case class Subject(id:Int, name:String)
  case class Score(id:Int, studentName:String, subjectId:Int, score:Double)

  def main(args: Array[String]): Unit = {
    //构建批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //分别将资料中的两个文件复制到项目中的data/join/input中
    //学科Subject（学科ID、学科名字）
    //成绩Score（唯一ID、学生姓名、学科ID、分数——Double类型）
    //分别使用readCsvFile加载csv数据源，并制定泛型
    val subjectDataSet = env.readCsvFile[Subject]("./data/join/input/subject.csv")
    val scoreDataSet = env.readCsvFile[Score]("./data/join/input/score.csv")

    //使用join连接两个DataSet，并使用where、equalTo方法设置关联条件
    val joinDataSet = scoreDataSet.join(subjectDataSet).where(_.subjectId).equalTo(_.id)

    //打印关联后的数据源
    joinDataSet.print()
  }
}
