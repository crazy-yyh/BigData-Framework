package com.yuhang.workcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yyh
 * @create 2021-05-17 16:45
 */
object WorkCount {


    def main(args: Array[String]): Unit = {

        // 建立和Spark框架的连接
        val conf = new SparkConf().setMaster("local").setAppName("wordcount")
        val context = new SparkContext(conf)

        // 1. 读取文件，获取一行一行的数据
        val lines: RDD[String] = context.textFile("datas")

        // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
        //    扁平化：将整体拆分成个体的操作
        //   "hello world" => hello, world, hello, world
        val words: RDD[String] = lines.flatMap(_.split(" "))


        // 3. 将数据根据单词进行分组，便于统计
        //    (hello, hello, hello), (world, world)
        val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(words => words)

        // 4. 对分组后的数据进行转换
        //    (hello, hello, hello), (world, world)
        //    (hello, 3), (world, 2)
        val wordToCount = wordGroup.map {
            case (word, list) => {
                (word, list.size)
            }
        }

        // 5. 将转换结果采集到控制台打印出来
        val array: Array[(String, Int)] = wordToCount.collect()
      
        array.foreach(println)

        //关闭连接
        context.stop()


    }


}
