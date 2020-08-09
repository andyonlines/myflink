package org.example.time

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

    //设置应用时间使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 系统每隔一分钟的机器时间插入一次水位线(周期性的)
    //默认是200毫秒
    env.getConfig.setAutoWatermarkInterval(60000)

    val stream = env.socketTextStream("xiaoai08", 9999, '\n')
        .map(line => {
          val str = line.split(" ")
          (str(0),str(1).toLong * 1000)
        })
      // 抽取时间戳和插入水位线
      // 插入水位线的操作一定要紧跟source算子
        .assignTimestampsAndWatermarks(
          // 最大延迟时间设置为5s
          new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
            //设置时间戳为element._2
            override def extractTimestamp(element: (String, Long)): Long = element._2
          }
        )
        .keyBy(_._1)
        .timeWindow(Time.seconds(10))
        .process(new MyProcess)

    stream.print()
    env.execute()
  }

  //当窗口关闭的时候执行
  class MyProcess extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit =
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + " 的窗口中共有 " + elements.size + " 条数据")
  }
}
