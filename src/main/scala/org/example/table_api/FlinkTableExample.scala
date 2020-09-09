package org.example.table_api

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//下面三个隐式转换一定有要导入
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}


object FlinkTableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.connect(new FileSystem().path("E:\\work\\java\\idea\\myflink\\src\\main\\resources\\sensor.txt"))
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable") // 创建临时表

    val sensorTable = tableEnv.from("inputTable")

    val resultTavle = sensorTable.select("id,temperature")
      .filter("id = 'sensor_1'")
      .toAppendStream[(String, Double)]
//      .print()

    val resultSqlTable = tableEnv
      .sqlQuery("select id,temperature from inputTable where id='sensor_1'")

    resultSqlTable.toAppendStream[(String,Double)]
      .print()

    env.execute()
  }
}
