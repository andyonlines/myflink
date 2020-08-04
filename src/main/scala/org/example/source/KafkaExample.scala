package org.example.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaExample {
  def main(args: Array[String]): Unit = {
    //构建env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092") //设置kafka的host

    props.put("group.id", "consumer-group") //这个设置group.id

    //设置props中key的序列化器
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserialization"
    )

    //设置props中value的序列化器
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserialization"
    )

    //创建一个KafkaConsumer座位flink的输入源
    val stream = env.addSource(new FlinkKafkaConsumer011[String](
      "test", //设置test是需要消费的topic
      new SimpleStringSchema(), //这里设置的是数据格式,在kafka中的数据是string,在网络中传输数流数据,那这里接收的时候
      //SimpleStringSchema会根据utf-8对流进行解码成string
      props //这里是设置属性
    ))

    //下面就可以对stream流进行计算等操作了
    stream.print()

    env.execute()
  }
}
