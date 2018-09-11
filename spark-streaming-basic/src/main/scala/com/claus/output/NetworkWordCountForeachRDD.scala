package com.claus.output

import java.sql.DriverManager

import com.claus.ConnectionPool
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

// output result to mysql database
object NetworkWordCountForeachRDD {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))
    val wordPairs = words.map(x => (x, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)

//    // 把结果保存到MySQL中 (错误)
//    wordCounts.foreachRDD { (rdd, time) =>
//      Class.forName("com.mysql.jdbc.Driver")
//      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_test", "root", "LvRaikkonen_0306")
//      val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
//      rdd.foreach { record =>
//        statement.setLong(1, time.milliseconds)
//        statement.setString(2, record._1)
//        statement.setInt(3, record._2)
//        statement.execute()
//      }
//      statement.close()
//      conn.close()
//    }


    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.zipWithIndex.foreach { case ((word, count), index) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
          if (index != 0 && index % 500 == 0) {
            statement.executeBatch()
            conn.commit()
          }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
    }

    ssc.start()

    ssc.awaitTermination()
  }

}
