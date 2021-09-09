package com.yuhang.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
            算子 - map
 */
object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map

        val rdd = sc.makeRDD(List(1,2,3,4))
        // 1,2,3,4
        // 2,4,6,8

        // 转换函数
        def mapFunction(num:Int): Int = {
            num * 2
        }

        //传入一个函数
//        val mapRDD: RDD[Int] = rdd.map(mapFunction)

        //使用匿名函数
        //val mapRDD: RDD[Int] = rdd.map((num:Int)=>{num*2})

        //函数逻辑只有一行，花括号可以省略
        //val mapRDD: RDD[Int] = rdd.map((num:Int)=>num*2)

        //如果参数类型可以自动推导，那么返回类型可以省略
        //val mapRDD: RDD[Int] = rdd.map((num)=>num*2)

        //参数列表只有一个，那么小括号可以省略
        //val mapRDD: RDD[Int] = rdd.map(num=>num*2)

        //参数只出现一次，并且按顺序出现，那么可以使用下划线代替
        val mapRDD: RDD[Int] = rdd.map(_*2)

        mapRDD.collect().foreach(println)

        sc.stop()

    }
}
