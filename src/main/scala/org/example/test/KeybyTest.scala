package org.example.test

import org.apache.flink.streaming.api.scala._
import org.example.source.self.SensorSource

import scala.util.Random


object KeybyTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val random = new Random()
    env.setParallelism(3)
    val parallelism = env.getParallelism

    val stream = env.addSource(new SensorSource).setParallelism(1)
      .map(v=>{
        (v.id,(random.nextInt().abs % 3).toString) //_.2 为分组字段
      })
//      .shuffle
      .keyBy(v=>v._2)
      .print()

    env.execute()
  }
}
