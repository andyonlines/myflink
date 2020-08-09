package org.example.windowfunc

import org.apache.flink.streaming.api.scala._
import org.example.source.self.SensorSource

/**
 * 每10条数据计算温度和
 */
object GuanCountWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
        .keyBy(0)
        .countWindow(10,5)
        .sum(2)
        .print()

    env.execute()
  }

}
