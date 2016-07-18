package output

import java.nio.file.Paths
import java.io._

import com.google.common.collect.BiMap
import datasource.SourceType.SourceType
import mlprocess.SVDItemSimilarityComputer
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import com.google.common.io.Files
import org.apache.spark.rdd.RDD

/**
  * Created by wushuang on 2016/7/18.
  */
class IntermediateStore(sparkContext: SparkContext,hdfsTemp:String,localTemp:String,_type:SourceType) {
  val objPath = new Path(Paths.get(hdfsTemp,_type.toString).toString,"uirdd")
  val objFile = new File(Paths.get(localTemp,_type.toString).toString,"temp.ser")
  try{
    Files.createParentDirs(objFile)
  }catch {
    case ex:IOException => throw new RuntimeException(ex)
  }
  def save(userRSIDIndexBiMap : BiMap[String,Int],
           itemRSIDIndexBiMap : BiMap[String,Int],
           computer : SVDItemSimilarityComputer,
           uiRDD : RDD[(Int, Int)]):Unit = {
    try{
      val fs = FileSystem.get(sparkContext.hadoopConfiguration)
      val objPathTemp = new Path(objPath.toString + "temp")
      if (!fs.exists(objPathTemp.getParent)) {
        fs.mkdirs(objPathTemp.getParent)
      }
      uiRDD.saveAsObjectFile(objPathTemp.toString)
      if (objFile.exists()) objFile.delete()
      if (fs.exists(objPath)) fs.delete(objPath,true)
      fs.rename(objPathTemp,objPath)
      val fout = new FileOutputStream(objFile)
      val oos = new ObjectOutputStream(fout)
      oos.writeObject(userRSIDIndexBiMap)
      oos.writeObject(itemRSIDIndexBiMap)
      oos.writeObject(computer)
      fout.close()
    }catch {
      case ex: IOException => throw new RuntimeException(ex)
    }
  }


}
