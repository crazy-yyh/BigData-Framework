package com.yuhang.api

import com.yuhang.api.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object WindowTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputPath = "..\\BigData-Framework\\Flink\\apiTest\\src\\main\\resources\\sensor.txt"

    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    val dataStream: DataStream[SensorReading] = inputStream.map(
      arr => {
        val data = arr.split(",")
        SensorReading(data(0).toString, data(1).toLong, data(2).toDouble)
      }
    )

    dataStream
      .keyBy("id")



  }

}
