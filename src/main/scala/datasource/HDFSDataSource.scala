package datasource

import java.io.File

import org.apache.spark.sql.SQLContext
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormatter
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

  val fmt = DateTimeFormat.forPattern("yyyyMMdd")
  /*
  返回对应资源的路径

  @return 数据源中包含的文件路径列表
  */
  def getPaths : mutable.Set[String]={
    var date = if (startDate != null) fmt.parseLocalDateTime(startDate) else null
    val paths = mutable.Set.empty[String]

    for(i<-1 to effectiveDays){
      val d: String = fmt.print(date)
      paths += (pathJoin(rootDir, "data_date=" + d + "/*"))

      date = date.minusDays(i)
    }
    return paths
  }
  def sourceType = SourceType
  private def pathJoin(p: String, c: String): String = new File(p, c).toString
}
