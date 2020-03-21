package com.socgen.bsc.cbs.businessviews.thirdParties.tools

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.thirdParties.config.DataSourceName.DataSourceName
import com.socgen.bsc.cbs.businessviews.thirdParties.config.ThirdPartiesBvConfig

/**
  * Created by X153279 on 12/02/2017.
  */
object DataFrameLoaderWithPrefix {

  def colPrefixed(ds: DataSourceName, colName: String): String = {
    val prefix = ThirdPartiesBvConfig.usedPrefix(ds)
    if (colName.startsWith(prefix))
      return colName
    prefix.concat(colName)
  }

  def loadWithPrefix(dataLoader: DataFrameLoader, ds: DataSourceName): DataFrame = {
    val dataSourceBvName = ThirdPartiesBvConfig.businessViewName(ds)
    withColumnPrefix(dataLoader.load(dataSourceBvName), ds)
  }

  def withColumnPrefix(df: DataFrame, ds: DataSourceName): DataFrame = {
    val columns = df.columns

    columns.foldLeft(df) {
      (df, colName) => (df.withColumnRenamed(colName, colPrefixed(ds, colName)))
    }
  }
}
