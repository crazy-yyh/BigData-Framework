package com.yuhang.rdd.create

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 探究数据在分区中如何分配保存
 * @author yyh
 * @create 2021-05-20 9:32
 */
object Spark01_RDD_Memory_Par1 {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD

        // 【1，2】，【3，4】
        //val rdd = sc.makeRDD(List(1,2,3,4), 2)
        // 【1】，【2】，【3，4】
        //val rdd = sc.makeRDD(List(1,2,3,4), 3)
        // 【1】，【2,3】，【4,5】
        val rdd = sc.makeRDD(List(1,2,3,4,5), 3)


        //底层就是这段源码来进行数据进入不同分区的方法def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
        //    if (numSlices < 1) {
        //      throw new IllegalArgumentException("Positive number of partitions required")
        //    }
        //    // Sequences need to be sliced at the same set of index positions for operations
        //    // like RDD.zip() to behave as expected
        //    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
        //      (0 until numSlices).iterator.map { i =>
        //        val start = ((i * length) / numSlices).toInt
        //        val end = (((i + 1) * length) / numSlices).toInt
        //        (start, end)
        //      }
        //    }
        //
        //      模式匹配
        //    seq match {
        //      case r: Range =>
        //        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
        //          // If the range is inclusive, use inclusive range for the last slice
        //          if (r.isInclusive && index == numSlices - 1) {
        //            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
        //          }
        //          else {
        //            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
        //          }
        //        }.toSeq.asInstanceOf[Seq[Seq[T]]]
        //      case nr: NumericRange[_] =>
        //        // For ranges of Long, Double, BigInteger, etc
        //        val slices = new ArrayBuffer[Seq[T]](numSlices)
        //        var r = nr
        //        for ((start, end) <- positions(nr.length, numSlices)) {
        //          val sliceSize = end - start
        //          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
        //          r = r.drop(sliceSize)
        //        }
        //        slices
        //      case _ =>
        //        val array = seq.toArray // To prevent O(n^2) operations for List etc
        //        positions(array.length, numSlices).map { case (start, end) =>
        //            array.slice(start, end).toSeq
        //        }.toSeq
        //    }
        //  }

        //重点   length:集合长度， numSlices: 分区数
        /*def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
            (0 until numSlices).iterator.map { i =>
                val start = ((i * length) / numSlices).toInt
                val end = (((i + 1) * length) / numSlices).toInt
                (start, end)
            }
        }*/

        //上面那个例子
        // (0 until numSlices)就是 0，1，2，不包含numSlices，循环迭代
        //0 => (0,1)
        //1 => (1,3)
        //2 => (3,5)   左闭右开

        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // TODO 关闭环境
        sc.stop()
    }
}
