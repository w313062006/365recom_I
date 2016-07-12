package model

import java.util

import com.google.common.collect.Iterables
import com.google.common.collect.Sets
import datasource.SourceType
import scala.collection.JavaConverters._
import org.apache.spark.{SparkContext, SparkConf}
import scala.reflect.runtime.universe._
import scala.collection.mutable

/**
  * Created by wushuang on 16-7-8.
  */
object test {
  def main(args: Array[String]) {

    val t:Array[String] = Array("123","3211","2222")
    val m = t.map(_.toInt)
    println(m)

  }

}
