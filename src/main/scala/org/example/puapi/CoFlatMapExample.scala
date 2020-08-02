package org.example.puapi

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    println(env.getParallelism)

    val one = env.fromElements((1, 1L))
      .setParallelism(1)

    val two = env.fromElements((1, "tow"))
      .setParallelism(1)

    val conn = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed = conn.flatMap(new MyCoFlatMap)

    printed.print()

    env.execute()
  }

  class MyCoFlatMap extends CoFlatMapFunction[(Int,Long),(Int,String),String]{
    override def flatMap1(value: (Int, Long), out: Collector[String]): Unit = {
      out.collect(value._2.toString + "来自第一条流")
    }

    override def flatMap2(value: (Int, String), out: Collector[String]): Unit = {

      out.collect(value._2.toString + "来自第二条流")
    }
  }


}
