package org.example.join.work

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.example.source.self.SensorSource

object NewJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(1)
    val stream: DataStream[(String, String, Double, Long)] = env
      .addSource(new SensorSource)
      .map(v => {
        val value = v.id.split("_")(1)
        (v.id,value,v.temperature,v.timestamp)
      })

stream.print()
//    val stream1: DataStream[(String, String)] = env.fromElements(("0","0"),("1","a"),("2","ab"),("3","abc"),("4","abcd"),("5","abcde"))

//    stream.join(stream1)
//      .where(_._2)
//      .equalTo(_._1)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
//      .apply(new FlatJoinFunction[(String, String, Double, Long),(String, String),String] {
//        override def join(in1: (String, String, Double, Long), in2: (String, String), collector: Collector[String]): Unit = {
//          collector.collect(in1 + " ==> " + in2)
//        }
//      }).print()

//      .map(v => {
//        val value = new java.util.HashMap[String, Object]()
//        value.put("id",v.id.split("_")(1))
//        value.put("timestamp",v.timestamp)
//        value.put("temperature",v.temperature)
//        value.put("temp_id",v.temperature)
//        val map = new java.util.HashMap[String, Object]()
//        map.put(v.id,value)
//        map
//      })
//      .print()
    env.execute()
  }
}
