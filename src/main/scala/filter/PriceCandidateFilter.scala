package filter

import com.google.common.collect.Sets
import model.{PriceType, Item}
import scala.collection.JavaConverters._

/**
  * Created by wushuang on 2016/7/18.
  */
class PriceCandidateFilter(priceSegmenterFactory: PriceSegmenterFactory) extends CandidateFilter{
  override def getCandidates(a:Item,all:Set[Item]):Set[Item] = {
    if (a.priceType ==PriceType.PriceUnknown || a.price == 0){
      collection.immutable.Set.empty[Item]
    }
    val segmenter = priceSegmenterFactory.getPriceSegmenter(a.cityName,a.priceType)
    if (segmenter == null){
      collection.immutable.Set.empty[Item]
    }
    val candidates = Sets.newHashSet[Item]()
    for (i<-all){
      if (segmenter.getStage(a.price) == segmenter.getStage(i.price) && i.price != 0){
        candidates.add(i)
      }
    }
    candidates.asScala.toSet
  }
}
