package mlprocess

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseMatrix,Matrix,Vector,Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, MatrixEntry, CoordinateMatrix}
import scala.math._
import java.io.Serializable;

/**
  * Created by wushuang on 16-7-15.
  */
class SVDItemSimilarityComputer extends Serializable{
  private var itemMatrix:DenseMatrix = null
  def train(uiRDD: RDD[(Int,Int)],nRows:Int,nCols:Int):Unit = {
    val ratingMatrix = new CoordinateMatrix(uiRDD.map(t => MatrixEntry(t._1,t._2,1)),nRows,nCols)
    val rowMatrix = ratingMatrix.toIndexedRowMatrix()
    val rowMatrix2 = new IndexedRowMatrix(rowMatrix.rows.cache(),rowMatrix.numRows(),rowMatrix.numCols().toInt)
    val vprime = rowMatrix2.computeSVD(9,false,0).V
    //使用rowmatrix可以直接使用normalizer的方法，但是后续的计算需要转换，暂时放弃自带API，自己实现norm的方法在Matrix格式上
    //val normMatrix = new Normalizer().transform(SVDItemSimilarityComputer.toRDD(vprime,uiRDD.sparkContext))
    //itemMatrix = new DenseMatrix(vprime.numRows,vprime.numCols,normMatrix)
    itemMatrix = new DenseMatrix(vprime.numRows, vprime.numCols, vprime.toArray)
    // 列向量归一化
    itemMatrix = normr(itemMatrix)

    //normr(new DenseMatrix(vprime.numRows,vprime.numCols,vprime.toArray))
  }
  def compute(idxA:Int,idxB:Int):Double={
    var sim:Double = 0
    for (i<-0 to itemMatrix.numCols-1){
      sim += (itemMatrix(idxA,i) * itemMatrix(idxB,i))
    }
    sim
  }
  def normr(v:Matrix):DenseMatrix = {
    val values:Array[Double] = v.toArray

    val m = v.numRows
    val n = v.numCols
    val normr = new Array[Double](n)
    for (i<-0 to m-1){
      var norm:Double = 0
      for (j<-0 to n-1){
        norm += math.pow(v.apply(i,j),2)
      }
      normr(i) = math.sqrt(norm)
    }
    for (i<-0 to m-1;j<-0 to n-1){
      //v.apply(i,j) /= normr(i)
      values(SVDItemSimilarityComputer.index(i,j)) /= normr(i)
    }
    new DenseMatrix(m,n,values)
  }
  object SVDItemSimilarityComputer{
    def toRDD(m: Matrix,sc: SparkContext): RDD[Vector] = {
      val columns = m.toArray.grouped(m.numRows)
      val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
      val vectors = rows.map(row => Vectors.dense(row.toArray))
      sc.parallelize(vectors)

    }
    def index(i: Int, j: Int): Int = {
      if (!itemMatrix.isTransposed) i +itemMatrix.numRows * j else j + itemMatrix.numCols * i
    }
  }
  //def normr
}
