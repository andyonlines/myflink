package org.example.puapi

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.fromElements((1, 100))
    val stream2 = env.fromElements((2, "tow"))

    val stream3 = stream1.connect(stream2)

    val stream4 = stream3.map(new MyCoMap)

    stream4.print()

    env.execute()
  }

  //定义CoMapFunction类
  class MyCoMap extends CoMapFunction[(Int,Int),(Int,String),String] {
    override def map1(value: (Int, Int)): String = {
      value._2.toString + "来自第一条流"
    }

    override def map2(value: (Int, String)): String = {
      value._2 + "来自第二条流"
    }
  }

}
