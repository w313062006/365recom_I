package databuild

import com.google.common.collect.BiMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
/**
  * Created by wushuang on 16-7-13.
  */
class LogDateFrameProcessor {
  def process(df:DataFrame):(BiMap[String,Int],BiMap[String,Int],RDD[Tuple2[Int,Int]]) = {
    val pairRDD:RDD[(String,String)] = df.rdd.map(t=> (t.getAs("USER_ID"),t.getAs("ITEM_ID")))
    //pairRDD.cache
    val userIds = pairRDD.keys.distinct().collect()
    //val userIds = pa
    val itemIds = pairRDD.values.distinct().collect()

    val userIndexBiMap:BiMap[String,Int] = null
    val itemIndexBiMap:BiMap[String,Int] = null

    for (i<- userIds.indices){userIndexBiMap.put(userIds(i),i)}
    for (i<- itemIds.indices){itemIndexBiMap.put(itemIds(i),i)}

    val userIndexMapBroadcast:Broadcast[BiMap[String,Int]] = df.sqlContext.sparkContext.broadcast(userIndexBiMap)
    val itemIndexMapBroadcast:Broadcast[BiMap[String,Int]] = df.sqlContext.sparkContext.broadcast(itemIndexBiMap)

    val uiRDD = pairRDD.map(t => (userIndexMapBroadcast.value.get(t._1),itemIndexMapBroadcast.value.get(t._2)))
    pairRDD.unpersist()
    new Tuple3(userIndexBiMap,itemIndexBiMap,uiRDD)

  }

}
