package com.claus.receiver

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object NetworkWordCount {

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)

    // Streaming 入口
    val ssc = new StreamingContext(sc, Seconds(20))

    // 数据接收器 receiver
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // 数据处理 process
    val words = lines.flatMap(_.split(" "))
    val wordPairs = words.map(x => (x, 1))
    val wordCount = wordPairs.reduceByKey(_ + _)

    // output
    wordCount.print()

    // 启动Streaming处理流
    ssc.start()

    // 等待Streaming程序终止
    ssc.awaitTermination()

  }

}
