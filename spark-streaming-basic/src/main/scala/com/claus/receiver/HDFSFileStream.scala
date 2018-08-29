package com.claus.receiver

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HDFSFileStream {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // streaming context
    val ssc = new StreamingContext(sc, Seconds(10))

    // hdfs file
    val filePath = "hdfs://localhost:9000/user/claus/streaming/filestream"

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](filePath,
      (path: Path) => path.toString.contains("process"), true).map(_._2.toString)

    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCount.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
