package org.example.watermark

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 应用程序使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 系统每隔一分钟的机器时间插入一次水位线
    env.getConfig.setAutoWatermarkInterval(60000)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        // 第二个元素是时间戳，必须转换成毫秒单位
        (arr(0), arr(1).toLong * 1000)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Long)): Long = {
          element._2
        }
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new Myprocess)

    stream.print()
    env.execute()

  }

  class Myprocess extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + " 的窗口中共有 " + elements.size + " 条数据")
    }
  }
}
