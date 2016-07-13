package datasource

import java.io.File

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable
/**
  * Created by wushuang on 16-7-9.
  */
class HDFSDataSource(_sqlContext: SQLContext, _rootDir: String, _effectiveDays: Int, _startDate: String,
                     _sourceType: SourceType.SourceType ) {
  val sqlContext = _sqlContext
  val rootDir = _rootDir
  val effectiveDays = _effectiveDays
  val startDate = _startDate
  val SourceType = _sourceType


  /*
  返回对应资源的路径

  @return 数据源中包含的文件路径列表
  */
  def getPaths : mutable.Set[String]={
    var date = if (startDate != null) HDFSDataSource.fmt.parseLocalDate(startDate) else null
    val paths = mutable.Set.empty[String]

    for(i<-1 to effectiveDays){
      val d: String = HDFSDataSource.fmt.print(date)
      paths += (pathJoin(rootDir, "data_date=" + d + "/*"))
      date = date.minusDays(i)
    }
    return paths
  }
  def sourceType = SourceType
  private def pathJoin(p: String, c: String): String = new File(p, c).toString

  //读取hdfs上的parquet数据
  def load : DataFrame = sqlContext.read.parquet(getPaths.toArray : _*)

}
object HDFSDataSource{
  val fmt = DateTimeFormat.forPattern("yyyyMMdd")
}
