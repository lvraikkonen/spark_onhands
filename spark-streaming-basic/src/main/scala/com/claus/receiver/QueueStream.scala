package com.claus.receiver

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue

object QueueStream {

  def main(args: Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // streaming context
    val ssc = new StreamingContext(sc, Seconds(100))

    // queue of RDDs
    val rddQueue = new Queue[RDD[Int]]()

    // 数据接收器 receiver QueueInputStream
    val inputStream = ssc.queueStream(rddQueue)

    // transform
    val mappedStream = inputStream.map(x => (x%10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    // output
    reducedStream.print()

    ssc.start()

    // 将RDD 追加到Queue中
    //rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)

    ssc.awaitTermination()
    //ssc.stop(false)

  }

}
