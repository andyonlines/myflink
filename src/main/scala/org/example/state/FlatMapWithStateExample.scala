package org.example.state

import org.apache.flink.streaming.api.scala._
import org.example.source.self.{SensorReading, SensorSource}
import org.example.state.ValueStateInFlatMap.TemperatureAlert

object FlatMapWithStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new SensorSource)
      .keyBy(_.id)
      // 第一个参数是输出的元素的类型，第二个参数是状态变量的类型
      .flatMapWithState[(String,Double,Double),Double]{
        case (in:SensorReading,None) =>{ //匹配第一个元素
          (List.empty,Some(in.temperature)) //只更新状态变量,不返回任何值到下游算子
        }
        case (in:SensorReading,lastTemp:Some[Double]) =>{ //状态中已经和有值
          var tempDiff = (in.temperature - lastTemp.get).abs
          if(tempDiff > 1.7){
            (List((in.id,in.temperature,tempDiff)),Some(in.temperature)) //输出结果到下游
          }else{
            (List.empty,Some(in.temperature)) //值更新状态
          }
        }

      }
      .print()

    env.execute()
  }
}
