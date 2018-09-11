package com.claus

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object InvertedIndex {

  def main(args: Array[String]): Unit ={
    val inputFile = "file:////Users/lvshuo/Desktop/tmp/input.data"
    val conf = new SparkConf().setAppName("InvertedIndex").setMaster("local")
    val sc = new SparkContext(conf)
    val result = sc.textFile(inputFile).flatMap(x => {
      val arr = x.split("\t")
      val arr_words = arr(1).split(" ")
      arr_words.map(y => (y, arr(0)))
    }
    ).reduceByKey((x, y) => x + "|" + y)
    result.foreach(println)
  }
}
