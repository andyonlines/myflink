package org.example.windowfunc

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.example.source.self.SensorSource

object AvgTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream.map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .aggregate(new AvgTempFunction)
      .print()

    env.execute()
  }

  class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Long), (String, Double)] {
    override def createAccumulator(): (String, Double, Long) = ("", 0.0, 0L)

    override def add(in: (String, Double), acc: (String, Double, Long)): (String, Double, Long) = {
      (in._1, in._2 + acc._2, acc._3 + 1)
    }

    override def getResult(acc: (String, Double, Long)): (String, Double) = {
      (acc._1, acc._2 / acc._3)
    }

    override def merge(acc: (String, Double, Long), acc1: (String, Double, Long)): (String, Double, Long) = {
      (acc._1, acc._2 + acc1._2, acc._3 + acc1._3)
    }
  }

}
