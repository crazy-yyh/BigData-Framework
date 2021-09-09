package com.yuhang.distributed_test

import java.io.InputStream
import java.net.{ServerSocket, Socket}

/**
 * @author yyh
 * @create 2021-05-19 9:03
 */
object Executor {

    def main(args: Array[String]): Unit = {

        val server = new ServerSocket(9999)
        println("服务器启动，等待连接")


        val socket: Socket = server.accept()

        val in: InputStream = socket.getInputStream

        val i: Int = in.read()

        println("接收的数据为"+ i)

        in.close()
        server.close()
    }

}
