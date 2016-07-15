package model

import java.util
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._

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

    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    val m = dm.toArray
    m.map(println)
    val columns = m.toArray.grouped(3)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    for (i<-vectors){
      println(i.toString())
    }
  }



}
