package org.example.watermark

import java.sql.Timestamp

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.source.self.{SensorReading, SensorSource}

/**
 * 实现整数秒计算输出
 */
object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .filter(r => r.id.equals("sensor_2"))
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger) //设置触发器,触发器的作用是控制触发计算下一个算子process的逻辑
      .process(new WindowResult)
      .print()

    env.execute()
  }

  /**
   * 实现ProcessWindowFunction
   */
  class WindowResult extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("传感器ID为 " + key + " 的传感器窗口中元素的数量是 " + elements.size)
    }
  }

  /**
   * 定义触发器类
   * SensorReading : 输入类型
   * TimeWindow : 窗口类型
   */
  class OneSecondIntervalTrigger extends Trigger[SensorReading,TimeWindow] {

    /**
     * 每来一条数据执行一次
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     */
    override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

      //定义一个状态变量
      // 是单例模型,只会创建一次,出事值为false
      val firstSenn = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      //判断是否为第一条元素来的时候
      if(!firstSenn.value()){
        //计算整数秒
        //// 假设第一条事件来的时候，机器时间是1234ms，t是多少？t是2000ms
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))

        //注册定时器
        ctx.registerProcessingTimeTimer(t)

        // 在窗口结束时间注册一个定时器
        ctx.registerEventTimeTimer(window.getEnd)

        //更新状态
        firstSenn.update(true)
      }

      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("回调函数触发时间：" + new Timestamp(time))
      if (time == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      } else {
        //注册一下一个整数秒的触发器
        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        if (t < window.getEnd) {
          ctx.registerProcessingTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    //方法会在窗口清除的时候调用
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      // SingleTon, 单例模式，只会被初始化一次
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }
}
