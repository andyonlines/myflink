package org.example.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.example.source.self.{SensorReading, SensorSource}

/**
 * 实现上一次温度和这次温度差如果超过1.7度,输出当前温度
 */
object ValueStateInFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new TemperatureAlert(1.7))
      .print()

    env.execute()
  }

  class TemperatureAlert(val diff:Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)] {
    var lastTemp: ValueState[Double] = _
    override def open(parameters: Configuration): Unit = {
      /**
       * 初始化一个状态变量保存上一次的温度
       */
      lastTemp = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
      )
    }
    override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
      val last = lastTemp.value() //获取上次温度

      //求这次温度和上次温度的温度差的绝对值
      val tempDiff = (in.temperature - last).abs

      //如果设置的差值,输出
      if (tempDiff > diff){
        collector.collect((in.id,in.temperature,tempDiff))
      }

      //更新状态变量
      lastTemp.update(in.temperature)
    }
  }
}
