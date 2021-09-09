package com.yuhang.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
/*
        (Key - Value类型) partitionBy 将数据按照指定 Partitioner 重新进行分区。 Spark 默认的分区器是 HashPartitioner
 */
object Spark14_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        val mapRDD:RDD[(Int, Int)] = rdd.map((_,1))
        // RDD => PairRDDFunctions
        // 隐式转换（二次编译）

        // partitionBy根据指定的分区规则对数据进行重分区
        val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
        newRDD.partitionBy(new HashPartitioner(2))

        //如果重分区的分区器和当前 RDD 的分区器一样怎么办？
        //会判断是否一样，也就是说，没区别，返回还是原先那个
        /*if (self.partitioner == Some(partitioner)) {
            self
        } else {
            new ShuffledRDD[K, V, V](self, partitioner)
        }*/


        //Spark 还有其他分区器吗？
        //RangePartitioner(SortByKey用的就是这个） PythonPartitioner(锁定）

        // 如果想按照自己的方法进行数据分区怎么办？
        //自己写个分区器


        newRDD.saveAsTextFile("output")




        sc.stop()

    }
}
