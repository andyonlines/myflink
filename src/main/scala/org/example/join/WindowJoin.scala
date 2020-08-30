package org.example.join

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orangeStream = env
      .fromElements((1, 1999L), (1, 2001L),
        (2, 1999L), (2, 2001L))
      .assignAscendingTimestamps(_._2)

    val greenStream = env
      .fromElements((1, 1001L), (1, 1002L), (1, 3999L))
      .assignAscendingTimestamps(_._2)

    val s = orangeStream.join(greenStream)
      .where(r => r._1) // 第一条流使用`_1`字段做keyBy
      .equalTo(r => r._1) // 第二条流使用`_1`字段做keyBy
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      //.apply { (e1, e2) => e1 + " *** " + e2 }
      .apply(new Myjoin)
      .print()

    env.execute()
  }

  class Myjoin extends FlatJoinFunction[(Int,Long),(Int,Long),String] {
    override def join(in1: (Int, Long), in2: (Int, Long), collector: Collector[String]): Unit = {
      collector.collect("input1 ==> " + in1 + "  input1 ==> " + in2)
    }
  }
}
