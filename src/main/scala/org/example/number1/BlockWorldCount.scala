package org.example.number1

import org.apache.flink.api.scala._

/**
 * 这是批处理
 */
object BlockWorldCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment //创建环境
    val input =
      env.readTextFile("E:\\work\\java\\idea\\myflink\\src\\main\\resources\\data1") //读取文件

    val value =
      input.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    value.print()
  }
}
