package com.claus.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD_File {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("File-RDD")
    val sc = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sc.textFile("input") // 读取文件默认采用Hadoop读取文件规则
    // 一行一行

    println(fileRDD.collect().mkString(","))

    sc.stop()

  }

}
