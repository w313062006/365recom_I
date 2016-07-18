package driver


import com.google.common.collect.{HashBiMap, BiMap}
import databuild.LogDateFrameProcessor
import datasource.SourceType.SourceType
import datasource.{AppLogTransformer, WebLogTransformer, HDFSDataSource, SourceType}
import filter._
import mlprocess.{JaccardItemRecommender, SVDItemSimilarityComputer}
import model.{ItemUtils, Item}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.LocalDate
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import scala.collection.JavaConverters._

/**
  * Created by wushuang on 16-7-13.
  */
class JobDriver {
  private val logger:Logger = LoggerFactory.getLogger(classOf[JobDriver])

  def run(jobConf:JobConfiguration): Unit = {
    //从数据库读取数据
    warmup(jobConf)



  }
  private def warmup(jobConfiguration: JobConfiguration)={
    jobConfiguration.getItems
    logger.info("房源个数(区分物业类型):" + jobConfiguration.getItems.size())
    if (jobConfiguration.getRecommendType == RecommendType.PRICE) jobConfiguration.getPriceSegmentsFactory
    if (jobConfiguration.getSourceType == SourceType.APP) jobConfiguration.getAppPageId
    if (jobConfiguration.getRecommendType == RecommendType.INCREMENTAL) jobConfiguration.getDBDataSource
    logger.info("作业配置加载完成")
  }

  private def constructJobName(jobConfiguration: JobConfiguration):String = {
    "RS365-" + jobConfiguration.getSourceType + "-" + jobConfiguration.getRecommendType
  }
  private def doOfflineRecommend(jobConfiguration: JobConfiguration):/*RecommendResult*/Unit = {
    //初始化spark上下文
    val conf = new SparkConf().setAppName(constructJobName(jobConfiguration))
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    //加载日志信息
    val source = constructHDFSDataSource(jobConfiguration,sqlcontext)
    val transformer = if (jobConfiguration.getSourceType == SourceType.WEB) new WebLogTransformer else
      new AppLogTransformer(jobConfiguration.getAppPageId.asScala.toList)
    val df = transformer.transform(source.load).coalesce(15)
    //将DataFrame转化为<User,Item>的RDD

    val result = new LogDateFrameProcessor().process(df)
    val userRSIDIndexBiMap = result._1
    val itemRSIDIndexBiMap = result._2
    val uiRDD = result._3
    uiRDD.cache()

    val indexItemBiMap = constructIndexItemBiMap(jobConfiguration.getSourceType,
                                                 jobConfiguration.getItems.asScala.toList,
                                                 itemRSIDIndexBiMap)

    logger.info("评分矩阵与数据库房源中的交集个数是" + indexItemBiMap.size())

    //训练SVD相似性计算器
    val computer = new SVDItemSimilarityComputer()
    computer.train(uiRDD,userRSIDIndexBiMap.size(),itemRSIDIndexBiMap.size())
    //如果当前是猜你喜欢，则保存中间结果
    if (jobConfiguration.getRecommendType == RecommendType.INTEREST){
      jobConfiguration.getIntermediateStore(sc).save(userRSIDIndexBiMap,itemRSIDIndexBiMap,computer,uiRDD)
    }
    //构建过滤器
    val filter = constructFilter(jobConfiguration)
    val recommender = new JaccardItemRecommender(sc,
                                                 userRSIDIndexBiMap,
                                                 itemRSIDIndexBiMap,
                                                 computer,
                                                 filter,
                                                 jobConfiguration)


  }

  /**
    *
    * @param jobConfiguration
    * @param sqlontext
    * @return
    */
  private def constructHDFSDataSource(jobConfiguration: JobConfiguration,sqlontext: SQLContext):HDFSDataSource ={
    var startDate:String = null
    if (jobConfiguration.isDebug) {startDate = jobConfiguration.getDebugStartDate
    } else{
      startDate = HDFSDataSource.fmt.print(new LocalDate().minusDays(1))
    }
    if (jobConfiguration.getSourceType == SourceType.WEB){
      new HDFSDataSource(sqlontext,
                        jobConfiguration.getWebDataDir,
                        jobConfiguration.getWebDataDays,
                        startDate,
                        SourceType.WEB)
    }else{
      new HDFSDataSource(sqlontext,
        jobConfiguration.getWebDataDir,
        jobConfiguration.getWebDataDays,
        startDate,
        SourceType.APP)
    }

  }

  def constructIndexItemBiMap(_type:SourceType, _items:List[Item],_itemRSIDIndexBiMap:BiMap[String,Int]) : BiMap[Int,Item] = {
    val indexItemBiMap:BiMap[Int,Item] = HashBiMap.create()
    for (i<- _items){
      val rsid = ItemUtils.getRSID(i,_type)
      if (_itemRSIDIndexBiMap.containsKey(rsid)) indexItemBiMap.put(_itemRSIDIndexBiMap.get(rsid),i)
    }
    indexItemBiMap
  }

  def constructFilter(jobConfiguration: JobConfiguration) : CandidateFilter = {
    val recommendType = jobConfiguration.getRecommendType
    recommendType match{
      case RecommendType.PRICE => new CandidateFilterChain(jobConfiguration.getRecommendNum,
                                                           new CityChannelPriceTypeCandidateFilter,
                                                           new SaleStateCandidateFilter,
                                                           new PriceCandidateFilter(jobConfiguration.getPriceSegmentsFactory))
      case RecommendType.LOCAL => new CandidateFilterChain(jobConfiguration.getRecommendNum,
                                                           new CityChannelPriceTypeCandidateFilter,
                                                           new SaleStateCandidateFilter,
                                                           new LocalCandidateFilter)
      case _ => new CandidateFilterChain(jobConfiguration.getRecommendNum,
                                         new CityChannelPriceTypeCandidateFilter,
                                         new SaleStateCandidateFilter)
    }
  }
}
