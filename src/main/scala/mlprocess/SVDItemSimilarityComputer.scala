package mlprocess

import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, MatrixEntry, CoordinateMatrix}

/**
  * Created by wushuang on 16-7-15.
  */
class SVDItemSimilarityComputer extends Serializable{
  private val itemMatrix:DenseMatrix = null
  def train(uiRDD: RDD[(Int,Int)],nRows:Int,nCols:Int):Unit = {
    val ratingMatrix = new CoordinateMatrix(uiRDD.map(t => MatrixEntry(t._1,t._2,1)),nRows,nCols)
    val rowMatrix = ratingMatrix.toIndexedRowMatrix()
    val rowMatrix2 = new IndexedRowMatrix(rowMatrix.rows.cache(),rowMatrix.numRows(),rowMatrix.numCols().toInt)
    val vprime = rowMatrix2.computeSVD(9,false,0).V

    //normr(new DenseMatrix(vprime.numRows,vprime.numCols,vprime.toArray))
  }
  def compute(idxA:Int,inxB:Int):Unit={

  }
  //def normr
}
