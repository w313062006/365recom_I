package filter

import java.io.Serializable

import model.PriceType.PriceType

/**
  * Created by wushuang on 16-7-12.
  * 价格分段工厂
  */
abstract class PriceSegmenterFactory extends Serializable{
  /**
    * 给定城市和物业,返回合适的价格分类器
    *
    */
    def getPriceSegmenter(_city:String,_type:PriceType):PriceSegmenter
}
