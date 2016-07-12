package datasource

import org.apache.spark.sql.DataFrame


/**
  * Created by wushuang on 16-7-11.
  */
class AppLogTransformer(appPageId:List[String]) extends LogTransformer{

  override def transform(frame: DataFrame):DataFrame = {
    var df_log = frame.filter(frame.col("page_id").isin(appPageId)).filter(frame.col("context_id").isNotNull)
    df_log = df_log.selectExpr("cast (device_id as string) as USER_ID","concat (city,'_',context_id) as ITEM_ID")
    //确定数据类不为空
    df_log = df_log.filter(df_log.col("USER_ID").isNotNull).filter(df_log.col("ITEM_ID").isNotNull)
    //去掉重复数据
    df_log = df_log.dropDuplicates()
    df_log
  }
}
