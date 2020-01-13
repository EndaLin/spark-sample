package com.dgut.sample

/**
 * @author linwt
 * @date 2019/11/27 10:20
 */
object HelloWorld {

  val multiplier: Int => Int = (i:Int) => i * i

  def main(args: Array[String]): Unit = {
    var greeting = "Hello World";
    println(add(1, 2).toString)
    println(multiplier(3).toString)
    array()
  }

  def add(v1: Int, v2: Int): Int = {
    v1 + v2
  }

  def array(): Unit = {
    var myList = new Array[String](3)
    myList(0) = "1"
    myList(1) = "2"
    myList(2) = "3"
    for (x <- myList) {
      println(x)
    }
  }
}
