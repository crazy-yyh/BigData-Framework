package com.yuhang.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @author yyh
 * @create 2021-09-09 16:33
 */
// 流处理word count
object wordcountstream {

    def main(args: Array[String]): Unit = {

        // 创建流处理的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //    env.setParallelism(8)
        //    env.disableOperatorChaining()

        // 从外部命令中提取参数，作为socket主机名和端口号
//        val paramTool: ParameterTool = ParameterTool.fromArgs(args)
//        val host: String = paramTool.get("host")
//        val port: Int = paramTool.getInt("port")

        // 接收一个socket文本流
        val inputDataStream: DataStream[String] = env.socketTextStream("192.168.117.100",9999)
        // 进行转化处理统计
        //
        val resultDataStream: DataStream[(String, Int)] = inputDataStream
                .flatMap(_.split(" "))
                .filter(_.nonEmpty)
                .map((_,1))
                .keyBy(0)
                .sum(1)

        resultDataStream.print().setParallelism(1)

        // 启动任务执行
        env.execute("stream word count")

    }

}
