package org.example.number1

import org.apache.flink.streaming.api.scala._ //注意这里需要用_,把该包的所有导入,因为有隐式转换
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
  def main(args: Array[String]): Unit = {
    //获取env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //添加源
    val inputstream =
      env.socketTextStream("xiaoai07", 9999, '\n')

    val windowCounts = inputstream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0) //分组

      .timeWindow(Time.seconds(5))//开一个5秒的窗口
      .sum(1) // 统计

    windowCounts.print()
    env.execute()
  }
}
