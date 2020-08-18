package org.example.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// nc -lk 9999
//a 1  设定定时器为 11秒,这时候的水位线为 long的最小值
//a 12 设定定时器为 22秒(这是有两个定时器),水位线为 12 - 11.999秒,定时器1会被触发
//a 23 设定定时器为 33秒(这是有两个定时器,有一个定时器已经被触发),水位线为 22.999秒,定时器2会被触发
object EventTimeOnTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("xiaoai01", 9999, '\n')
        .map(line => {
          val arr = line.split(' ')
          (arr(0),arr(1).toLong * 1000L)
        })
        .assignAscendingTimestamps(_._2)//指定水位线字段
        .keyBy(_._1) //分区
        .process(new MyKeyedProcess)
        .print()


    env.execute()
  }

  /**
   * [String,(String,Long),String]
   * 1.key值类型
   * 2.in 值类型
   * 3.out值类型
   *
   *
   */
  class MyKeyedProcess extends KeyedProcessFunction[String,(String,Long),String] {

    /**
     * 每来一条数据调用一次
     * @param value 输入的值
     * @param ctx
     * @param out
     */
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      /**
       * 在当前元素时间戳的10s钟以后，注册一个定时器，定时器的业务逻辑由`onTimer`函数实现
       * 时间到了之后会回调
       */
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L)
      out.collect("当前的水位线是: " + ctx.timerService().currentWatermark())
    }


    /**
     * 该函数是在定时器被触发的时候调用
     * 因为现在程序是使用eventtime,所以这里的时间都是用eventtime来表示
     * 比如设定定时器的时间为为10秒钟触发,那么当水位先到达10秒才会触发
     * 如果水位线线不到达10秒,那么无论机器时间多大都不会触发定时器
     * @param timestamp 定时器的时间(如果是event time,那么timestamp也是eventtime)
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {

      out.collect("位于时间戳：" + timestamp + "的定时器触发了！")
    }
  }
}
