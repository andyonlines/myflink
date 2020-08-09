package org.example.windowfunc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.example.source.self.SensorSource

/**
 *这是会话窗口的例子
 * 如果20秒钟没有数据来,那么窗口关闭
 */
object SessionWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
        .keyBy(0)
        .window(EventTimeSessionWindows.withGap(Time.seconds(20)))
        .sum(2)
        .print()

    env.execute()
  }

}
