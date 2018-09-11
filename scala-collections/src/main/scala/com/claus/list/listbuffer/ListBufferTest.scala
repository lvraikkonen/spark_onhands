package com.claus.list.listbuffer

object ListBufferTest {

  def main(args: Array[String]): Unit = {

    val list = List(1, 2, 3)

    // 在list尾部插入一个元素
    import scala.collection.mutable.ListBuffer

    val buf = new ListBuffer[Int]
    buf += 1
    buf += 2
    // ListBuffer(1, 2)

    3 +=: buf //在前面插入  ListBuffer(3, 1, 2)

  }

}
