package com.claus.list.highorder

object High {

  def main(args: Array[String]): Unit = {

    // map, flatMap, foreach
    List(1,2,3) map (_ + 1)
    List(1,2,3).map(x => x + 1)

    val words = List("hello", "world", "scala!")
    words.map(_.length)
    words.map(_.toList.reverse.mkString)

    words.flatMap(_.toList)

    var sum = 0
    List(1, 2 ,3, 4, 5).foreach(sum += _)


    // filter, partition, find, takeWhile, dropWhile, span
    List(1,2,3,4,5).filter(x => x%2==0)
    words.filter(x => x.length == 5)

    List(1,2,3,4,5).partition(x => x%2 == 0)

    List(1,2,3,4,5).find(x => x%2 == 0) // 找到的第一个

    List(1,2,3,-4,5).span(_ > 0)

    List(1,-3, 4, 2, 6).sortWith((a:Int, b: Int) => a>b) // 降序
    words.sortWith(_.length > _.length) // 按照单词长度降序

    // reduce
    val list = List(1,7,2,9)
    list.reduce((a, b) => a+b)
    list.reduce(_ + _)

    list.fold(0)((a, b) => a+b)

    (List(10, 20), List(3, 4, 5)).zipped.map((a, b) => a*b) // List(30, 80)
    (List("abc", "de"), List(3, 2)).zipped.forall(_.length == _)
    (List("abc", "de"), List(3, 2)).zipped.exists(_.length != _)
  }

}
