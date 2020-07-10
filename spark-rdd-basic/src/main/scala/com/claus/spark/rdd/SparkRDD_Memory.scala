package com.claus.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD_Memory {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("createRDD")
    val sc = new SparkContext(sparkConf)

    val sampleData = List(1,2,3,4)
    val rdd: RDD[Int] = sc.parallelize(sampleData)

    rdd.collect().foreach(println)

    sc.stop()
  }

}
