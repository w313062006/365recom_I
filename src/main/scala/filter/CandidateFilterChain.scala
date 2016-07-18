package filter

import model.Item
import scala.collection
/**
  * Created by wushuang on 2016/7/18.
  */
class CandidateFilterChain(leastRecommendNum:Int,filter:CandidateFilter*) extends CandidateFilter{
  val delegates = filter.toList
  override def getCandidates(a:Item,all:Set[Item]) : Set[Item] = {
    if (delegates != null){
      val candidates = all
      for (i<-delegates){
        val temp = i.getCandidates(a,candidates)
        if (temp.size <= leastRecommendNum) return candidates
        else return temp
      }
    }
    scala.collection.immutable.Set.empty
  }
}
