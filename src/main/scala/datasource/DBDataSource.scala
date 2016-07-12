package datasource

import java.sql.Connection
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.BeanListHandler

import scala.collection.JavaConverters._

/**
  * Created by wushuang on 16-7-8.
  */
class DBDataSource(dbC:Connection,sqlString:String) {
  private val dbConn = dbC
  private val sql = sqlString

  def load: List[UIPair] = {
    val runner = new QueryRunner()
    val pairs = runner.query(dbConn,sql,new BeanListHandler(classOf[UIPair]))
    pairs.asScala.toList
  }
}
