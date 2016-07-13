package driver

import java.io.File
import java.util

import datasource.{DBDataSource, SourceType}
import datasource.SourceType.SourceType
import driver.RecommendType.RecommendType
import filter.{PriceSegmenter, PriceSegmenterFactory}
import model.PriceType.PriceType
import model.{PriceType, Item}
import model.PriceType.PriceType
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.configuration.ConfigurationException
import com.google.common.base.{MoreObjects, Supplier, Throwables, Suppliers}

import java.sql.{DriverManager, SQLException, Connection}
import org.apache.commons.dbutils.handlers.{ColumnListHandler, BeanListHandler}
import org.apache.commons.dbutils.{BasicRowProcessor, QueryRunner, ResultSetHandler}
import com.google.common.collect.{Table,HashBasedTable}


/**
  * Created by wushuang on 16-7-12.
  */
class JobConfiguration(_sourceType: SourceType,_recommendType:RecommendType,configFile:File){
  /*
  若在调试模式下运行作业,使用定义好的数据源作为开始日期
  格式为"20160101"
   */
  private final val KEY_DEBUG_STARTDATE = "debug.startdate"
  private final val KEY_IS_DEBUG = "debug"

  //Web端历史数据在HDFS上的根目录
  private final val KEY_WEB_DIR = "web.dir"
  //web端数据使用天数
  private final val KEY_WEB_DAYS = "web.days"
  //App端历史数据在HDFS上的根目录
  private final val KEY_APP_DIR = "app.dir"
  //App端数据的使用天数
  private final val KEY_APP_DAYS = "app.days"
  //增量计算时HDFS上存放中间结果的目录
  private final val KEY_INCREMENTAL_IMSTORE_HDFS = "inc.im.hdfs"
  //增量计算中本地存放中间结果的目录
  private final val KEY_INCREMENTAL_IMSTORE_LOCAL = "inc.im.local"
  //web端增量数据库的url
  private final val KEY_INCREMENTAL_WEB_DB_URL = "inc.web.db.url"
  //web端增量数据库的用户名
  private final val KEY_INCREMENTAL_WEB_DB_USER = "inc.web.db.user"
  //web端增量数据库的密码
  private final val KEY_INCREMENTAL_WEB_DB_PASSWD = "inc.web.db.passwd"
  //web增量数据库的sql
  private final val KEY_INCREMENTAL_WEB_SQL = "inc.web.db.sql"
  //app端增量数据库的url
  private final val KEY_INCREMENTAL_APP_DB_URL = "inc.app.db.url"
  //app端增量数据库的用户名
  private final val KEY_INCREMENTAL_APP_DB_USER = "inc.app.db.user"
  //app端增量数据库的密码
  private final val KEY_INCREMENTAL_APP_DB_PASSWD = "inc.app.db.passwd"
  //app增量数据库读取USER_ID,ITEM_ID的SQL
  private final val KEY_INCREMENTAL_APP_SQL = "inc.app.db.sql"
  //结果推送时,使用的redis服务器host
  private final val KEY_REDIS_HOST = "redis.host"
  //结果推送时,使用的redis服务器的port
  private final val KEY_REDIS_PORT = "redis.port"
  //房源属性数据库的url
  private final val KEY_DB_URL = "db.url"
  //房源属性数据库的user
  private final val KEY_DB_USER = "db.user"
  //房源属性数据库的passwd
  private final val KEY_DB_PASSWD = "db.passwd"

  //价格分段器的城市列表
  private final val KEY_PRICE_SEGMENTS_CITY = "ps.citys"
  /*
  间隔分段器的前缀,对每个城市,其价格分段器格式如下:
  PREFIX+城市名+类型
  如
  ps.南京.sqm = 8000,10000,15000,20000,30000
  ps.南京.house = 200,300,500,700,1000
   */
  private final val KEY_PRICE_SEGMENTS_PREFIX = "ps."
  private final val KEY_PRICE_SEGMENTS_PRICEPERSQM = ".sqm"
  private final val KEY_PRICE_SEGMENTS_PRICEPERHOUSE = ".house"
  //默认价格分段器对应的城市名称
  private final val KEY_PRICE_SEGMENTS_DEFAULT_CITY = "default"

  //推荐结果个数
  private final val KEY_RECOMMEND_NUMBER = "recommend.number"

  private var sourceType:SourceType = null
  private var recommedType:RecommendType = null
  private var config:PropertiesConfiguration = null
  private var segmenterFactory:PriceSegmenterFactory = null

  private final var itemSupplier:Supplier[util.List[Item]] = null
  private final var appPageIdSupplier:Supplier[util.List[String]] = null
  private final var incrementalConnectionSupplier:Supplier[Connection] = null

  sourceType = _sourceType
  recommedType = _recommendType
  config = new PropertiesConfiguration()
  config.setDelimiterParsingDisabled(true)
  try{
    config.load(configFile)
  }catch {
      //错误继续抛出
    case ex : ConfigurationException => throw Throwables.propagate(ex)
  }

  val propertyConnectionSupplier = Suppliers.memoize(new ConnectionSupplier(config.getString(KEY_DB_URL),
                                                                            config.getString(KEY_DB_USER),
                                                                            config.getString(KEY_DB_PASSWD)
  ))

  if (sourceType == SourceType.WEB){
    this.incrementalConnectionSupplier = Suppliers.memoize(new ConnectionSupplier(config.getString(KEY_INCREMENTAL_WEB_DB_URL),
                                                                                  config.getString(KEY_INCREMENTAL_WEB_DB_USER),
                                                                                  config.getString(KEY_INCREMENTAL_WEB_DB_PASSWD)
    ))
  }else{
    this.incrementalConnectionSupplier = Suppliers.memoize(new ConnectionSupplier(config.getString(KEY_INCREMENTAL_APP_DB_URL),
                                                                                  config.getString(KEY_INCREMENTAL_APP_DB_USER),
                                                                                  config.getString(KEY_INCREMENTAL_APP_DB_PASSWD)
    ))
  }
  this.itemSupplier = Suppliers.memoize(new DBQuerySupplier(propertyConnectionSupplier.get(),"select WP.PRJ_ID as prjId, CI.CITY_ID_WEB_IP as cityId,CI.CITY_NAME as cityName, CI.CITY_KEY as cityKey, WC.ITEMNAME as name, WP.PRJ_LISTID as listId,"
    + " WC.CHANNEL as channel, WP.PRJ_DIST as dist,WP.PRJ_BLOCK as block,WP.MAP_X as mapx,"
    + " WP.MAP_Y as mapy,WC.PRICE as price,WC.PRICE_MORE as priceMore, WC.SALESTAT as salestats"
    + " from dwb_web_w_channel WC  inner join dwb_web_project WP on WC.PRJ_ID = WP.PRJ_ID inner join dwd_city_info CI on WP.PRJ_CITY = CI.CITY_ID",
    new BeanListHandler(classOf[Item],new BasicRowProcessor())
  ))

  this.appPageIdSupplier = Suppliers.memoize(new DBQuerySupplier(propertyConnectionSupplier.get(),
                                                                 "select PAGE_ID from dwd_app_page_info t where t.channel_type = 1",
                                                                 new ColumnListHandler[String](1)
  ))



  def getRecommendNum = config.getInt(KEY_RECOMMEND_NUMBER)
  def getSourceType = sourceType
  def getRecommendType = recommedType
  def isDebug = config.getBoolean(KEY_IS_DEBUG)
  def getDebugStartDate = config.getString(KEY_DEBUG_STARTDATE)
  def getWebDataDir = config.getString(KEY_WEB_DIR)
  def getWebDataDays = config.getInt(KEY_WEB_DAYS)
  def getAppDataDir = config.getString(KEY_APP_DIR)
  def getAppDataDays = config.getString(KEY_APP_DAYS)
  def getRedisHost = config.getString(KEY_REDIS_HOST)
  def getRedisPort = config.getInt(KEY_REDIS_PORT)

  override def toString = {
    MoreObjects.toStringHelper(this.getClass).add("SourceType",sourceType).add("RecommendType",recommedType).add("Configuration File",config.getFile)
    .toString
  }

  /**
    * 所有房源对应的item
    */
  def getItems: util.List[Item] = itemSupplier.get()
  def getAppPageId:util.List[String] = appPageIdSupplier.get()

  def getPriceSegmentsFactory:PriceSegmenterFactory = {
    if (segmenterFactory == null){
      val table:Table[String,PriceType,PriceSegmenter] = HashBasedTable.create()
      val citys = config.getString(KEY_PRICE_SEGMENTS_CITY).split(",").toList += KEY_PRICE_SEGMENTS_DEFAULT_CITY
      //citys += KEY_PRICE_SEGMENTS_DEFAULT_CITY
      for(i<-citys){
        var segs: Array[String] = config.getString(KEY_PRICE_SEGMENTS_PREFIX + i + KEY_PRICE_SEGMENTS_PRICEPERSQM).split(",")
        table.put(i,PriceType.PricePerSQM,new ArrayPriceSegmenter(segs.map(_.toInt)))

        segs = config.getString(KEY_PRICE_SEGMENTS_PREFIX + i + KEY_PRICE_SEGMENTS_PRICEPERHOUSE).split(",")
        table.put(i,PriceType.PricePerHouse,new ArrayPriceSegmenter(segs.map(_.toInt)))
      }
      segmenterFactory = new TablePriceSegmenterFactory(table)
    }
    segmenterFactory
  }
  /**
    *
    * @param url
    * @param username
    * @param passwd
    */

  class ConnectionSupplier(url:String,username:String,passwd:String) extends Supplier[Connection] {
    override def get : Connection = {
      try{
        DriverManager.getConnection(url,username,passwd)
      }catch {
        case ex: SQLException => throw new RuntimeException(ex)
      }
    }
  }

  class DBQuerySupplier[T](connection:Connection,sql:String,handler:ResultSetHandler[T]) extends Supplier[T]{
    //val t:ResultSetHandler[T] = handler
    override def get = {
      val runner = new QueryRunner()
      try {
        runner.query(connection, sql, handler)
      }catch{
        case ex:SQLException => throw new RuntimeException(ex)
      }

    }
  }

  private class ArrayPriceSegmenter(segments:Array[Int]) extends PriceSegmenter{
    override  def getStage(price:Int):Int ={
      for (i<-segments.indices){
        if (price < segments(i)) i
      }
      segments.length-1
    }
  }
  private class TablePriceSegmenterFactory(table:Table[String,PriceType,PriceSegmenter]) extends PriceSegmenterFactory{
    override def getPriceSegmenter(city:String,_type:PriceType):PriceSegmenter={
      if (table.containsRow(city)) table.get(city,_type)
      else table.get(KEY_PRICE_SEGMENTS_DEFAULT_CITY,_type)
    }
  }
  def getDBDataSource:DBDataSource = {
    if (sourceType == SourceType.WEB) new DBDataSource(incrementalConnectionSupplier.get(),config.getString(KEY_INCREMENTAL_WEB_SQL))
    else new DBDataSource(incrementalConnectionSupplier.get(),config.getString(KEY_INCREMENTAL_APP_SQL))
  }
}
