package org.example.windowfunc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.source.self.SensorSource

/**
 * 全量聚合函数计算平均温度
 */
object AvgTempPerWindowByProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    stream
      .map(r => (r.id, r.temperature))
      .keyBy(_._1 +"1")
      .timeWindow(Time.seconds(5))
      .process(new AvgTempFunc)
      .print()

    env.execute()
  }

  class AvgTempFunc extends ProcessWindowFunction[(String, Double), (String, Double), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[(String, Double)]): Unit = {
      val size = elements.size
      var sum: Double = 0.0
      for (r <- elements) {
        sum += r._2
      }
      out.collect((key, sum / size))
    }
  }
}