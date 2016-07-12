package filter

import java.io.Serializable
/**
  * Created by wushuang on 16-7-12.
  * 价格分段器,给定价格,返回所在的
  */
abstract class PriceSegmenter extends Serializable{
  def getStage(price:Int) : Int
}
