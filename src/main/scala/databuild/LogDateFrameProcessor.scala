package databuild

import com.google.common.collect.BiMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by wushuang on 16-7-13.
  */
class LogDateFrameProcessor {
  def process(df:DataFrame):Tuple3[BiMap[String,Int],BiMap[String,Int],RDD[Tuple2[Int,Int]]] = {
    val pairRDD = df.rdd.map(t => (t.getAs("USER_ID"),t.getAs("ITEM_ID")))
    pairRDD.cache
    val userids = pairRDD.keys.distinct().collect()
    val itemids = pairRDD.values.distinct().collect()

    val userIndexBiMap:BiMap[String,Int] = null
    val itemIndexBiMap:BiMap[String,Int] = null

    for (i<- userids.indices){userIndexBiMap.put(userids(i),i)}
    for (i<- itemids.indices){itemIndexBiMap.put(itemids(i),i)}

    val userIndexMapBroadcast:Broadcast[BiMap[String,Int]] = df.sqlContext.sparkContext.broadcast(userIndexBiMap)
    val itemIndexMapBroadcast:Broadcast[BiMap[String,Int]] = df.sqlContext.sparkContext.broadcast(itemIndexBiMap)

    val uiRDD = pairRDD.map(t => (userIndexMapBroadcast.value.get(t._1),itemIndexMapBroadcast.value.get(t._2)))
    pairRDD.unpersist()
    new Tuple3(userIndexBiMap,itemIndexBiMap,uiRDD)

  }

}
