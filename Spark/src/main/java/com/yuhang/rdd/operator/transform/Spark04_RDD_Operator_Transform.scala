package com.yuhang.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
    flatMap 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
    将 List(List(1,2),List(4,5))进行扁平化操作
 */
object Spark04_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - flatMap
        val rdd: RDD[List[Int]] = sc.makeRDD(List(
            List(1, 2), List(3, 4),List(5)
        ))
        val flatRDD: RDD[Int] = rdd.flatMap(
            list => {                  //list指的是List(1,2),List(3,4)
                list                    // 将数据封装成List
            }
        )
        flatRDD.collect().foreach(println)



        sc.stop()

    }
}
