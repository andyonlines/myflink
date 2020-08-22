package org.example.join


import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.example.source.self.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowedLatenessExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val reading = env.addSource(new SensorSource).assignAscendingTimestamps(_.timestamp)

    val countStream = reading.keyBy(_.id)
      .timeWindow(Time.seconds(10)) //开窗
      .allowedLateness(Time.seconds(5)) //窗口延时5秒更新数据
      /**
       * 如果真的有用迟到数据,每个窗口触发两次process
       * 一次是水位线超过窗口大小
       * 第二次是水位线超过allowedLateness
       */
      .process(new UpdatingWindowCountFunction)
      .print()

    env.execute()
  }


  class UpdatingWindowCountFunction extends ProcessWindowFunction[SensorReading,(String, Long, Int, String), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[(String, Long, Int, String)]): Unit = {
      val cnt = elements.count(_ => true)

      /**
       * 状态变量是用来记录是都否是水位线超过allowedLateness引发的process
       */
      val isUpdate = context.windowState.getState(
        new ValueStateDescriptor[Boolean](
          "isUpdate",
          Types.of[Boolean]
        )
      )


      if (!isUpdate.value()) {
        out.collect((key,context.window.getEnd,cnt, "frist")) //是水位线超过allowedLateness引发的process
        isUpdate.update(true)
      } else {
        out.collect( (key,context.window.getEnd,cnt,"update"))//不是是水位线超过allowedLateness引发的process
      }

    }
  }
}
