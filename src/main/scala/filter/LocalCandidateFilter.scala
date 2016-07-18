package filter

import model.Item

/**
  * Created by wushuang on 2016/7/18.
  */
class LocalCandidateFilter extends CandidateFilter{
  override def getCandidates(a:Item,all:Set[Item]):Set[Item] = {
    val temp:Set[Item] = null
    for (i<-all){
      if (a.block == i.block) (temp + i)
    }
    temp
  }
}
