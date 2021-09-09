package com.yuhang.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  map-小案例:从服务器日志数据 apache.log 中获取用户请求 URL 资源路径
 */
object Spark01_RDD_Operator_Transform_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

//        // TODO 算子 - map-小案例
//        val rdd = sc.textFile("datas/apache.log")
//
//        // 长的字符串
//        // 短的字符串
//        val mapRDD: RDD[String] = rdd.map(
//            line => {
//                val datas = line.split(" ")
//                datas(6)
//            }
//        )
//        mapRDD.collect().foreach(println)
//
//        sc.stop()

//
//        val rdd = sc.parallelize(Array(("spark",2),("hadoop",6),("hadoop",4),("spark",6)))
//
//        rdd.map()

    }
}
