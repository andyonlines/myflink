package org.example.puapi

import org.apache.flink.streaming.api.scala._
import org.example.source.self.SensorSource

object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)
        .filter(r => r.id.equals("sensor_1"))


    val stream2 = env.addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_2"))


    val stream3 = env.addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_3"))


    //把三条流合并成一条
    val unionStream = stream.union(
      stream2,
      stream3
    )



    unionStream.print()

    env.execute()
  }
}
