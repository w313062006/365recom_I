package filter

import model.Item

/**
  * Created by wushuang on 2016/7/18.
  */
trait CandidateFilter {
  /**
    * 给定目标房源和房源列表，返回符合要求的推荐房源
    * @param a 目标房源
    * @param all 所有候选的房源
    * @return 最终经过筛选的房源列表
    */
  def getCandidates(a:Item,all:Set[Item]):Set[Item]
}
