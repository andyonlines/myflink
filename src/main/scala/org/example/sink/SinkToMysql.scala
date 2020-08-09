package org.example.sink

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.example.source.self.{SensorReading, SensorSource}

object SinkToMysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)


    stream.addSink(new MysqlSink)


    env.execute()
  }

  class MysqlSink extends RichSinkFunction[SensorReading]{
    var conn: Connection =_
    var insertStatement: PreparedStatement = _
    var updateStatement: PreparedStatement = _
    override def open(parameters: Configuration): Unit = {
      //建立连接
      conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/mytest",
        "root",
        "asdfgh"
      )

      //声明语句
      insertStatement= conn.prepareStatement(
        "INSERT INTO flink_sink_test (id, timestamp, temperature) VALUES (?, ?, ?)"
      )


      //声明语句
      updateStatement= conn.prepareStatement(
        "UPDATE flink_sink_test SET temperature = ? AND timestamp = ? WHERE id = ?"
      )

    }


    //执行
    override def invoke(value: SensorReading): Unit = {
//      updateStatement.setObject(3,value.id)
//      updateStatement.setObject(2,value.timestamp)
//      updateStatement.setObject(1,value.temperature)
//      updateStatement.execute()
      insertStatement.setObject(1,value.id)
      insertStatement.setObject(2,value.timestamp)
      insertStatement.setObject(3,value.temperature)
      insertStatement.execute()


//      if (updateStatement.getUpdateCount == 0){
//        insertStatement.setObject(1,value.id)
//        insertStatement.setObject(2,value.timestamp)
//        insertStatement.setObject(3,value.temperature)
//        insertStatement.execute()
//      }
    }


    //释放资源
    override def close(): Unit = {
      if (updateStatement != null)
        updateStatement.close()
      if (insertStatement != null)
        insertStatement.close()
      if (conn != null)
        conn.close()
    }
  }

}
