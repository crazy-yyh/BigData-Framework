package com.yuhang.distributed_test

import java.io.OutputStream
import java.net.Socket

/**
 * @author yyh
 * @create 2021-05-19 9:03
 */
object Driver {


    def main(args: Array[String]): Unit = {

        val client = new Socket("localhost", 9999)

        val out: OutputStream = client.getOutputStream

        out.write(2)
        out.flush()
        out.close()

        client.close()
    }

}
