package driver

import com.beust.jcommander.JCommander
import datasource.SourceType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException

import collection.JavaConversions._
/**
  * Created by wushuang on 16-7-11.
  */
class CLI {
  private final val logger:Logger = LoggerFactory.getLogger(classOf[CLI])

  def main(args: Array[String]): Unit = {
    val options = new JobOptions

    try{
      val parser = new JCommander(options,args.toArray: _*)
      //parser.parse(args:_*)
      if(options.help) {
        parser.usage()
        return
      }
      if(options.sourceType == SourceType.APP && (options.recommendType == RecommendType.LOCAL || options.recommendType == RecommendType.PRICE)){
        logger.error("App数据只能用于猜你喜欢推荐")
        return
      }
      logger.info(options.toString)

    }catch {
      case ex:Exception => logger.error(ex.getMessage,ex)
    }

  }
}
