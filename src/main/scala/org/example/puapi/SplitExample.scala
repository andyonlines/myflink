package org.example.puapi

import org.apache.flink.streaming.api.scala._

object SplitExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      (1001, "1001"),
      (999, "999")
    )


    //把流分成两条流,实际上还是一条流,只是把大于1000打上"large"标签,
    //把小于等于1000的打上"small"标签
    val splitStream = stream.split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

    //把相应标签的数据领出来生成一条全新的流
    val largeStream = splitStream.select("large")
    val smallStream = splitStream.select("small")

    val allStream = splitStream.select("large","small")


    largeStream.print()
    env.execute()
  }
}
