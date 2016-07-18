package filter

import com.google.common.collect.Sets
import model.Item
import scala.collection.JavaConverters._

/**
  * Created by wushuang on 2016/7/18.
  */
class SaleStateCandidateFilter extends CandidateFilter{
  override def getCandidates(a:Item,all:Set[Item]):Set[Item] = {
    val candidates = Sets.newHashSet[Item]()
    for (i<-all){
      if (i.salestat == 2 || i.salestat == 3) candidates.add(i)
    }
    candidates.asScala.toSet
  }
}
