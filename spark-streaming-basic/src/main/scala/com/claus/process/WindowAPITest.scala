package com.claus.process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowAPITest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowAPI")
    val sc = new SparkContext(conf)

    // Streaming 入口
    val ssc = new StreamingContext(sc, Seconds(1)) // batch interval 1s

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // window process
    // 每隔1秒处理，显示长度为3秒的窗口数据
    val windowedDStream = lines.window(Seconds(3), Seconds(1))
    windowedDStream.print()

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x, 1))
    // 窗口长度20秒，滑动间隔5秒，每隔5秒统计前面20秒的数据
    val wordCounts = pairs.reduceByKeyAndWindow((a:Int, b:Int)=>(a+b), Seconds(20), Seconds(5))

    wordCounts.print()

    // 启动Streaming处理流
    ssc.start()

    // 等待Streaming程序终止
    ssc.awaitTermination()
  }

}
