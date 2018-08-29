package com.claus.process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyAPITest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowAPI")
    val sc = new SparkContext(conf)

    // Streaming 入口
    val ssc = new StreamingContext(sc, Seconds(1)) // batch interval 1s
    // set checkpoint
    ssc.checkpoint("hdfs://localhost:9000/user/claus/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // window process
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x, 1))

    pairs.updateStateByKey((values: Seq[Int], currentState: Option[Int]) =>
      Some(currentState.getOrElse(0) + values.sum)).print()

    // 启动Streaming处理流
    ssc.start()

    // 等待Streaming程序终止
    ssc.awaitTermination()
  }

}
