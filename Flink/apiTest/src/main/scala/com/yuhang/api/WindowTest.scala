package com.yuhang.api

import com.yuhang.api.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

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
        .map( data => (data.id,data.temperature))
      .keyBy(_._1)     // 按照二元组的第一个元素（id）分组
//      .window(TumblingEventTimeWindows.of(Time.seconds(15)))   // 滚动时间窗口
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3)))    // 滑动时间窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))       // 会话窗口
//      .timeWindow(Time.seconds(15))
      .countWindow(5)


  }

}
