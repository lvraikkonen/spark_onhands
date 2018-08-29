package com.claus.process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object JoinAPITest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)

    // Streaming 入口
    val ssc = new StreamingContext(sc, Seconds(10))

    val lines1 = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs1 = lines1.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    val lines2 = ssc.socketTextStream("localhost", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs2 = lines2.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

//    kvs1.join(kvs2).print()
//    kvs1.fullOuterJoin(kvs2).print()
    kvs1.leftOuterJoin(kvs2).print()

    // 启动Streaming处理流
    ssc.start()

    // 等待Streaming程序终止
    ssc.awaitTermination()
  }

}
