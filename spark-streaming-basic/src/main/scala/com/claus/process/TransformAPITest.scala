package com.claus.process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformAPITest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("TransformAPI")
    val sc = new SparkContext(conf)

    // Streaming 入口
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines1 = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs1 = lines1.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    val path = "hdfs://localhost:9000/user/claus/streaming/keyvalue.txt"
    val keyValueRDD = sc.textFile(path).map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    // 暴露DStream底层的RDD操作
    kvs1.transform { rdd =>
      rdd.join(keyValueRDD) // rdd 的相关transformation
    } print()

    // 启动Streaming处理流
    ssc.start()

    // 等待Streaming程序终止
    ssc.awaitTermination()
  }

}
