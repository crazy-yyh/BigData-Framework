package com.yuhang.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/*
    mapPartitionsWithIndex  分区索引，比如我只想要第二个分区里的数据
 */
object Spark03_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        // 【1，2】，【3，4】
        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator     //空的迭代器
                }
            }
        )
        mpiRDD.collect().foreach(println)


        sc.stop()

    }
}
