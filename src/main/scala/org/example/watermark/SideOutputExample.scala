package org.example.watermark

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.source.self.{SensorReading, SensorSource}

/**
 * 把大于32度的温度输出到侧输出流
 */
object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .process(new FreezingMonitor)

    //获取侧输出流
    //在这里不会创建新的侧输出流,因为会根据id找有没有freezing-alarms
    //如果有不会创建
    stream.getSideOutput(new OutputTag[String]("freezing-alarms"))
        .print()

    env.execute()
  }

  class FreezingMonitor extends ProcessFunction[SensorReading,SensorReading] {

    /**
     * 定义一条侧输出流
     * lazy 赖加载,需要用到才会被创建
     */
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

    /**
     * 数据每来一条会执行一次
     * @param value 来的数据的值
     * @param ctx
     * @param out
     */
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature > 32.0){
        ctx.output(freezingAlarmOutput,s"传感器ID为 ${value.id} 的传感器发出低于32华氏度的低温报警！")
      }

      out.collect(value) //输出到主流上
    }
  }

}
