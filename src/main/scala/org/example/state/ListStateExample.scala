package org.example.state

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.source.self.{SensorReading, SensorSource}

import scala.collection.mutable.ListBuffer

/**
 * 每10秒统计信息的数量并输出
 */
object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyKeyedProcess)
      .print()
    env.execute()
  }

  class MyKeyedProcess extends KeyedProcessFunction[String, SensorReading, String] {
    var listState: ListState[SensorReading] = _
    var timeTs: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {

      //定义一个状态列表
      listState = getRuntimeContext.getListState(
        new ListStateDescriptor[SensorReading]("list-state",
          Types.of[SensorReading]
        )
      )

      //定义一个状态值
      timeTs = getRuntimeContext.getState(
        new ValueStateDescriptor[Long](
          "timer",
          Types.of[Long]
        )
      )

    }

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value) //把元素添加到状态列表中
      if (timeTs.value() == 0L) { //第一个元素
        val ts = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts) //注册一个定时器
        timeTs.update(ts)
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      val list: ListBuffer[SensorReading] = ListBuffer() // 初始化一个空列表
      import scala.collection.JavaConversions._ // 必须导入
      // 将列表状态变量的数据都添加到列表中
      for (r <- listState.get()) {
        list += r
      }
      listState.clear() // gc列表状态变量

      out.collect("列表状态变量中的元素数量有 " + list.size + " 个")
      timeTs.clear()

    }
  }



}