package datasource

import org.apache.spark.sql.DataFrame
/**
  * Created by wushuang on 16-7-8.
  */
trait LogTransformer {
  /*
  将frame转化为包含有USER_ID,ITEM_ID两列的dataFrame
   */

  def transform(frame: DataFrame):DataFrame

}
