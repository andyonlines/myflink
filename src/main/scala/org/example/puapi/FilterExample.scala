package org.example.puapi

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
import org.example.source.self.{SensorReading, SensorSource}

/**
 * 利用filter算子把id为1的数据过滤出来
 */

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)
        //方法1
//      .filter(_.id.equals("sensor_1"))

      //方法2
//        .filter(new FilterFunction[SensorReading] {
//          override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
//        })

    //方法3
        .filter(new MyFilterFunction)
        .print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean =  value.id.equals("sensor_1")
  }

}
