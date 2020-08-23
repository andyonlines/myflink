package org.example.number1

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {
  def main(args: Array[String]): Unit = {
    //获取env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val backend = new FsStateBackend("hdfs://xiaoai08:9000/flink/flink1/checkouts/test1")
    env.setStateBackend(backend)
    //添加源
    val inputstream =
      env.socketTextStream("xiaoai07", 9999, '\n')

    val windowCounts = inputstream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0) //分组

//      .timeWindow(Time.seconds(5))//开一个5秒的窗口
      .sum(1) // 统计

    windowCounts.print()
    env.execute()
  }
}
