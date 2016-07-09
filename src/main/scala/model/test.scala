package model

import datasource.SourceType
/**
  * Created by wushuang on 16-7-8.
  */
object test {
  def main(args: Array[String]) {
    val b = SourceType.APP
    a(b)
  }
  def a (i :SourceType.SourceType) = {
    println(i)
  }
}
