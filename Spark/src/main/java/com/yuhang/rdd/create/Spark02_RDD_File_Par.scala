package com.yuhang.rdd.create

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 探究分区数量以及默认分区数量
 */

object Spark02_RDD_File_Par {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
        //     minPartitions : 最小分区数量
        //     math.min(defaultParallelism, 2)，因为配置了local[*],根据电脑核数，那么默认分区取最小值，也就是2
//        val rdd = sc.textFile("datas/1.txt")




        //像下面这个例子，虽然设置了分区数量为2，但是实际输出分区数量确实3，所以说2只能说是最小分区数，真正的分区数量可能会大
        val rdd = sc.textFile("datas/3.txt", 2)

        // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
        // Spark读取文件，底层其实使用的就是Hadoop的读取方式
        // 分区数量的计算方式：
        // long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits);
        //    totalSize = 7
        //    goalSize =  7 / 2 = 3（byte）    7：文件字节数，2：设置分区数 3：每个分区存放多少字节
        //   hadoop的1.1倍，  7/3 余 1 ， 1/3=33.3% 大于1.1倍，产生新分区，如果小于，不新增
        //    7 / 3 = 2...1 (1.1) + 1 = 3(分区)

        rdd.saveAsTextFile("output")


        // TODO 关闭环境
        sc.stop()
    }
}
