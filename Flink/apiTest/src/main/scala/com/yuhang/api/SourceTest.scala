package com.yuhang.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object SourceTest {

  //定义样例类
  case class SensorReading(id: String,timestamp: Long, temperature: Double)

  def main(args: Array[String]): Unit = {

    //定义环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )

    //从集合中读取数据
//    val stream1 = env.fromCollection(dataList);

    //从文件读取数据
//    val inputPath = "..\\BigData-Framework\\Flink\\apiTest\\src\\main\\resources\\sensor.txt"
//    val stream1 = env.readTextFile(inputPath)

    //从kafka中读取数据
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")

    val stream1 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //TODO 打印的顺序是乱序，默认并行度。待解决,根运行架构有关
    stream1.print();

    env.execute("source test")

  }

}
