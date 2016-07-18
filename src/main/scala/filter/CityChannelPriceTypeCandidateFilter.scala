package filter

import com.google.common.collect.Sets
import model.Item

import scala.collection.JavaConverters._

/**
  * Created by wushuang on 2016/7/18.
  */
class CityChannelPriceTypeCandidateFilter extends CandidateFilter{
  override def getCandidates(a:Item,all:Set[Item]):Set[Item] ={
    val candidates = Sets.newHashSet[Item]()
    for (i:Item <- all){
      if (i.equals(a) && i.cityId == a.cityId && i.priceType == a.priceType && i.channel == a.channel){
        candidates.add(i)
      }
    }
    candidates.asScala.toSet
  }
}
