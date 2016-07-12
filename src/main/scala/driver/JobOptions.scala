package driver

import com.beust.jcommander.Parameter
import com.google.common.base.MoreObjects

import java.io.File

import datasource.SourceType.SourceType

/**
  * Created by wushuang on 16-7-11.
  */
class JobOptions {

  @Parameter(required = true, names = Array("-source"), description = "数据源类型")
  var _sourceType: SourceType = null

  @Parameter(required = true, names = Array("-type"),
    description = "推荐类型: \n\t\t price->同价位 \n\t\t local->同区域 \n\t\t interest ->猜你喜欢 \n\t\t incremental ->增量")
  var _recommendType:RecommendType.RecommendType = null

  @Parameter(required = true, names = Array("-config"), description = "配置文件路径")
  var _configFile:File = null

  @Parameter(required = true, names = Array("--help"), description = "显示帮助")
  var _help:Boolean = false

  def sourceType = _sourceType
  def sourceType_=(value:SourceType):Unit = _sourceType = value

  def recommendType = _recommendType
  def recommendType_=(value:RecommendType.RecommendType):Unit = _recommendType = value

  def configFile = _configFile
  def configFile_=(value:File):Unit = _configFile = value

  def help = _help
  def help_=(value:Boolean):Unit = _help = value
  override def toString:String={
    MoreObjects.toStringHelper(classOf[JobOptions])
      .add("SourceType",sourceType)
      .add("RecommendType",recommendType)
      .add("Configuration",configFile)
      .toString
  }
}
