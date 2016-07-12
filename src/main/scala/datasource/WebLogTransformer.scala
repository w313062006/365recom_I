package datasource

import org.apache.spark.sql.DataFrame

/**
  * Created by wushuang on 16-7-11.
  */
class WebLogTransformer extends LogTransformer {
  override def transform(frame: DataFrame):DataFrame = {
    //确定数据范围
    var df_log = frame.filter(frame.col("ga_channel") isin("newhouse", "villa")).filter(frame.col("ga_contextid").isNotNull)
    //确定数据列
    df_log = df_log.selectExpr("cast(guid as string) as USER_ID","concat(cityid,'_',ga_contextid) as ITEM_ID")
    //确定数据类不为空
    df_log = df_log.filter(df_log.col("USER_ID").isNotNull).filter(df_log.col("ITEM_ID").isNotNull)
    //去掉重复数据
    df_log = df_log.dropDuplicates()
    df_log
  }
}
