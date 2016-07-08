package model

import datasource.SourceType
import datasource.SourceType.SourceType

/**
  * Created by wushuang on 16-7-8.
  */
object ItemUtils {
  def getRSID(x:Item,sourceType: SourceType): String = {
    if (sourceType == SourceType.WEB){
      return "" + x.cityId + "_" + x.channel + "_" + x.listId
    }else{
      return "" + x.cityName + "_" + x.channel + "_" + x.listId
    }
  }
}
