package org.example.project

import java.lang
import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UV {
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val stream = env
      .readTextFile("E:\\work\\java\\idea\\myflink\\src\\main\\resources\\UserBehavior.csv")
      .map(value =>{
        val arr = value.split(",")
        UserBehavior(arr(0).toLong,
          arr(1).toLong,
          arr(2).toLong,
          arr(3),
          arr(4).toLong)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp*1000)
      .map(r=>("key",r.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new UvProcessFunction)
      .print()

    env.execute()
  }

  class UvProcessFunction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      var s:Set[Long] = Set()

      for(e <- elements){
        s += e._2
      }

      out.collect("窗口" + new Timestamp(context.window.getStart) + "---" + new Timestamp(context.window.getEnd) + "的UV数是：" + s.size)
    }
  }
}
