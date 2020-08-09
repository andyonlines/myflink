package org.example.puapi

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._
import org.example.source.self.{SensorReading, SensorSource}

object KeyByExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)
      //1. 根据位置
      //.keyBy(0)
      // 2. 根据属性
      //.keyBy("id")

      //3. 或者传入匿名函数
//      .keyBy(_.id)

      //4. 自己定义KeySelector
      .keyBy(new KeySelector[SensorReading,String] {
        override def getKey(value: SensorReading): String = value.id
      })

      .sum(2)
      .print()

    env.execute()
  }

}
