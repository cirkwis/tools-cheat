package com.socgen.bsc.cbs.businessviews.common

import org.apache.spark.sql.DataFrame

/**
  * Created by X153279 on 13/04/2017.
  */
object ExtraFunctionsDF {
  def eliminateDuplicates(df: DataFrame, column: String): DataFrame = {
    df.dropDuplicates(Seq(column))
    /*
    val rdd = df.map (r => (r.getAs[String] (column), r) )
    .reduceByKey ((row1, row2) => row1)
    .map(_._2)
    df.sqlContext.createDataFrame (rdd, df.schema)

    */
  }
}
