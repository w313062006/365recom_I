package output

import com.google.common.cache.{CacheLoader, CacheBuilder}
import datasource.SourceType
import datasource.SourceType.SourceType
import driver.RecommendType
import driver.RecommendType.RecommendType
import model.Item

/**
  * Created by wushuang on 16-7-13.
  */
class RecommendResult(recommendTable:Map[Item,List[ItemWrapper]],sourceType: SourceType,recommendType: RecommendType) {
  private var sid:Int =_
  //this.recommendTable = Collections.unmodifiableSortedMap(recommendTable)
  if (sourceType == SourceType.APP && (recommendType == RecommendType.INTEREST || recommendType == RecommendType.INCREMENTAL)) sid = 4
  else if (recommendType == RecommendType.INTEREST || recommendType == RecommendType.INCREMENTAL) sid = 3
  else if (recommendType == RecommendType.LOCAL) sid = 2
  else if (recommendType == RecommendType.PRICE) sid = 1
  else throw new RuntimeException("unreachable")

  val keyCache = CacheBuilder.newBuilder().build(new CacheLoader[Item,String] {
    override def load(item:Item):String = {
      item.cityKey + "_" + item.channel + "_" + sid + "_" + item.listId
    }
  })
  val valueCache = CacheBuilder.newBuilder().build(new CacheLoader[ItemWrapper,String] {
    override def load(itemWrapper: ItemWrapper):String ={
      itemWrapper.item.listId + "_" + itemWrapper.getvalue
    }
  })


}
