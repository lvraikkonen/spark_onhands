package com.claus

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object AnalysisGithubLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Github push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputPath = "file:////Users/lvshuo/Desktop/hands_on/spark_onhands/test_data/2015-03-01-0.json"
    val ghLog = spark.read.json(inputPath)

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count
    val ordered = grouped.orderBy(grouped("count").desc)

    ordered.show(20)
  }
}
