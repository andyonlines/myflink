package org.example.number1

import org.apache.flink.api.scala._

/**
 * 这是批处理的wordcount
 */
object PiWordCount {
  def main(args: Array[String]): Unit = {
    //获取批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("E:\\work\\java\\idea\\myflink\\src\\main\\resources\\data1")
      .flatMap(_.split(" ")) //分割
      .map((_,1))//
      .groupBy(0)//分组
      .sum(1)//统计

    stream.print()//输出
  }
}
