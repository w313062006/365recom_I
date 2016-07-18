package mlprocess

import com.google.common.collect.BiMap
import driver.JobConfiguration
import filter.CandidateFilter
import org.apache.spark.SparkContext

/**
  * Created by wushuang on 2016/7/18.
  */
class JaccardItemRecommender(sparkContext: SparkContext,
                             userRSIDIndexBiMap : BiMap[String,Int],
                             itemRSIDIndexBiMap : BiMap[String,Int],
                             computer : SVDItemSimilarityComputer,
                             filter : CandidateFilter,
                             jobConfiguration: JobConfiguration) {

}
