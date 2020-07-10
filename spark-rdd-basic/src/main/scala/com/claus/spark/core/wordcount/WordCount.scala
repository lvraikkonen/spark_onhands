package com.claus.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // prepare spark environment
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCountDemo")
    val sc = new SparkContext(sparkConf)

    // transform
    val fileRDD: RDD[String] = sc.textFile("input")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    val wordCountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)

    // action
    val wordCount: Array[(String, Int)] = wordCountRDD.collect()
    wordCount.foreach(println)

    sc.stop()
  }

}
