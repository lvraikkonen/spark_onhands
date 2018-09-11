package com.claus.output

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object NetworkWordCountToHDFS {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OutputToHDFS").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))
    val wordPairs = words.map(x => (x, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)

    // saveASHadoopFiles 接口 输出到HDFS中
    wordCounts.repartition(1).mapPartitions { iter =>
      val text = new Text()
      iter.map { x=>
        text.set(x.toString())
        (NullWritable.get(), text)
      }
    } saveAsHadoopFiles[TextOutputFormat[NullWritable, Text]](
      "hdfs://localhost:9000/user/claus/streaming/wordcount", "-hadoop")

//    sc.sequenceFile("hdfs://localhost:9000/user/claus/streaming/wordcount*",
//      classOf[NullWritable], classOf[Text]).map(_._2.toString).collect()

    ssc.start()

    ssc.awaitTermination()
  }

}
