package org.example.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.example.source.self.{SensorReading, SensorSource}

/**
 * 把数据写到kafka中
 */
object KafkaSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)
        .map(one => {
          one.id + " " + one.timestamp + " " + one.temperature
        })


    stream.addSink(new FlinkKafkaProducer011[String](
      "xiaoai07:9092,xiaoai08:9092,xiaoai09:9092",
      "flink_test",
      new SimpleStringSchema()
    ))

    env.execute()
  }
}
