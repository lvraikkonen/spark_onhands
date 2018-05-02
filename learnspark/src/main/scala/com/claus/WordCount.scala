package com.claus

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object WordCount {
  def main(args: Array[String]): Unit = {
    val inputFile = "file:////Users/lvshuo/Desktop/tmp/hello.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).
      map(word => (word, 1)).
      reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}
