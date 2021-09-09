package com.yuhang.workcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yyh
 * @create 2021-05-20 9:06
 */
object WordCount2 {


    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordcount2")
        val context: SparkContext = new SparkContext(conf)

        val lines: RDD[String] = context.textFile("datas")

        val words: RDD[String] = lines.flatMap(_.split(" "))

        // 3. 将单词进行结构的转换,方便统计
        // word => (word, 1)
        val wordToOne: RDD[(String, Int)] = words.map(words => (words, 1))

        // 4. 将转换后的数据进行分组聚合
        // 相同key的value进行聚合操作
        // (word, 1) => (word, sum)
        val value: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

        val array: Array[(String, Int)] = value.collect()
        array.foreach(println)


    }

}
