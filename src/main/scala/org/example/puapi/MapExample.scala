package org.example.puapi

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._
import org.example.source.self.{SensorReading, SensorSource}

/**
 * 利用map算子把流中的SensorReading,转换成元组.
 */
object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)

      //方法1,往map传入匿名函数,函数是传入一个参数,出来一个参数
//      .map(obj => (obj.id,obj.temperature,obj.timestamp))


      //方法二 传入一个实现MapFunction接口的匿名类
      /**
       * [SensorReading,(String,Double,Long)] : 传入类型是SensorReading
       * 返回类型是(String,Double,Long)
       * 根据逻辑来写map函数
       */
//        .map(new MapFunction[SensorReading,(String,Double,Long)] {
//          override def map(value: SensorReading) = {
//            (value.id,value.temperature,value.timestamp)
//          }
//        })
//

      //方法3
        .map(new MyMapFunction)


    stream.print()

    env.execute()
  }

  //方法3,自定义类实现MapFunction接口
  class MyMapFunction extends MapFunction[SensorReading,(String,Double,Long)] {
    override def map(value: SensorReading): (String, Double, Long) = {
      (value.id,value.temperature,value.timestamp)

    }
  }

}
