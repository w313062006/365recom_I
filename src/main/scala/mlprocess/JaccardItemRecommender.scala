package mlprocess

import com.google.common.collect.{Iterables, Iterators, HashBiMap, BiMap}
import datasource.SourceType.SourceType
import driver.JobConfiguration
import filter.CandidateFilter
import model.{Item, ItemUtils}
import org.apache.hadoop.util.hash.Hash
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import output.RecommendResult


/**
  * Created by wushuang on 2016/7/18.
  */
class JaccardItemRecommender(sparkContext: SparkContext,
                             userRSIDIndexBiMap : BiMap[String,Int],
                             itemRSIDIndexBiMap : BiMap[String,Int],
                             computer : SVDItemSimilarityComputer,
                             filter : CandidateFilter,
                             jobConfiguration: JobConfiguration) {


  def recommend (ratings:RDD[(Int,Int)]) : Unit ={
    val indexItemBiMap = constructIndexItemBiMap(jobConfiguration.getSourceType,
                                                 jobConfiguration.getItems,
                                                 itemRSIDIndexBiMap)
    val ratingsGroupByItem = ratings.groupBy(t => t._2)
    val numVisiterPerItem = ratingsGroupByItem.map(t => (t._1,t._2.size))

  }

  def constructIndexItemBiMap(sourceType: SourceType,
                              items: List[Item],
                              itemRSIDIndexBiMap: BiMap[String,Int]): BiMap[Int,Item] ={
    val indexItemBiMap:HashBiMap[Int,Item] = HashBiMap.create()
    for (i<- items){
      val rsid = ItemUtils.getRSID(i,sourceType)
      if (itemRSIDIndexBiMap.containsKey(rsid)) indexItemBiMap.put(itemRSIDIndexBiMap.get(rsid),i)
      }
    indexItemBiMap
  }


}
