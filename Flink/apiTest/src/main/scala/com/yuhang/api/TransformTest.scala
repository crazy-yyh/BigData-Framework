package com.yuhang.api

import com.yuhang.api.SourceTest.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 0.读取数据
    val inputPath = "..\\BigData-Framework\\Flink\\apiTest\\src\\main\\resources\\sensor.txt"

    val inputData = env.readTextFile(inputPath)

    // 1.先转换成样例类类型（简单转换操作）  map
    val dataStream = inputData.map(
      arr => {
        val data = arr.split(",")
        SensorReading(data(0).toString, data(1).toLong, data(2).toDouble)
      }
    )

    // 2.分组聚合，输出每个传感器当前最小值
    val aggStream = dataStream
      .keyBy("id")
//      .min("temperature")  //min的话，就是找出Temperature的最小值，但是其他字段并不是当前最小字段的所属行
        .minBy("temperature")

    // 3.需要输出当前最小的温度值，以及最近的时间戳，要用reduce
    val resultStream = dataStream
      .keyBy("id")
//        .reduce((curData,newData) => {
//          SensorReading(curData.id,newData.timestamp,curData.temperature.min(newData.temperature))
//        })
        .reduce(new MyReduceFunction())

    // 4. 多流转换操作
    // 4.1 分流，将传感器温度数据分成低温、高温两条流
    val splitStream: SplitStream[SensorReading] = dataStream
        .keyBy("id")
        .split(data => {
          if (data.temperature > 30.0) Seq("hight") else Seq("low")
        })

    val hightStream: DataStream[SensorReading] = splitStream.select("hight")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    val allStream: DataStream[SensorReading] = splitStream.select("hight", "low")


    hightStream.print("hight")
//    lowStream.print()

    allStream.print("all")

    env.execute("transform test")




  }

}

class MyReduceFunction extends ReduceFunction[SensorReading]{
  override def reduce(curData: SensorReading, newData: SensorReading): SensorReading = {
    SensorReading(curData.id,newData.timestamp,curData.temperature.min(newData.temperature))
  }
}
