package org.example.project

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TopHotItems {
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)

  case class ItemViewCount(itemId: Long,
                           windowStart: Long,
                           windowEnd: Long,
                           count: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("E:\\work\\java\\idea\\myflink\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new CountAgg,new WindowResult)

    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }
  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getStart, context.window.getEnd, elements.head))
    }
  }
}
