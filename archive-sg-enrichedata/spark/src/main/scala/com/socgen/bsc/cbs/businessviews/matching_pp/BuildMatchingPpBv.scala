package com.socgen.bsc.cbs.businessviews.matching_pp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}

/**
  * Created by X156170 on 07/08/2017.
  */
class BuildMatchingPpBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override def name: String = "matching_pp_bv"

  override val frequency: Frequency.Value = Frequency.DAILY

  override def process(): DataFrame = {
    val grcPpId = dataLoader.load("grc_pp_bv").selectExpr("identifier as grc_pp_bv")
    val prvBvId = dataLoader.load("privalia_pp_bv").selectExpr("identifier as privalia_pp_bv")
    grcPpId.join(prvBvId,col("grc_pp_bv").equalTo(col("privalia_pp_bv")),"outer")
  }
}
