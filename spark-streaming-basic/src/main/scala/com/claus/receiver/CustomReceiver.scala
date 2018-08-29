package com.claus.receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CustomReceiver {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val lines = ssc.receiverStream(new CustomReceiver("localhost", 9876))
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCount.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop(false)
  }

}

class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  override def onStart(): Unit = {
    // 启动进程接收数据
    new Thread("Socket Receiver"){
      override def run(){ receive() }
    }.start()
  }

  override def onStop(): Unit = {

  }

  // create a customize socket connection and receive data until receiver is stopped
  private def receive(): Unit ={

    var socket: Socket = null
    var userInput: String = null
    try {
      // connect to host:port
      socket = new Socket(host, port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
      userInput = reader.readLine()
      while(!isStopped() && userInput != null){
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      restart("Trying to connect again")

    }catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}