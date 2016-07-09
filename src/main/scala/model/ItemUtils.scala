package model

import datasource.SourceType
import datasource.SourceType.SourceType

/**
  * Created by wushuang on 16-7-8.
  */
object ItemUtils {
  def getRSID(x:Item,sourceType: SourceType): String = {
    if (sourceType == SourceType.WEB) "" + x.cityId + "_" + x.channel + "_" + x.listId
    else "" + x.cityName + "_" + x.channel + "_" + x.listId
  }
}
