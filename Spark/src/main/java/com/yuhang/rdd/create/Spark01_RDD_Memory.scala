package com.yuhang.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 如何从内存中创建RDD
 * @author yyh
 * @create 2021-05-20 9:32
 */
object Spark01_RDD_Memory {

    def main(args: Array[String]): Unit = {

        //local表示模拟单核，local[*]表示模拟当前电脑核数
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // 从内存中创建RDD，将内存中集合的数据作为处理的数据源
        val seq = Seq[Int](1,2,3,4)

        // parallelize : 并行
        //val value: RDD[Int] = sc.parallelize(seq)

        // makeRDD方法在底层实现时其实就是调用了rdd对象的parallelize方法。
        val rdd: RDD[Int] = sc.makeRDD(seq)




        rdd.collect().foreach(println)

        sc.stop()


    }

}
