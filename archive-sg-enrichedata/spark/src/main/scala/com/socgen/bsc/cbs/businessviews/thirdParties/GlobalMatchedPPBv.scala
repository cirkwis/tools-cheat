package com.socgen.bsc.cbs.businessviews.thirdParties

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}

/**
  * The current BV acts as a bigMatching process for PP.
  * To be deleted once the process is set up.
  *
  * @param dataLoader
  */
class GlobalMatchedPPBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val name = "global_matched_pp_bv"
  override val frequency = Frequency.DAILY

  val sqlContext = dataLoader.sqlContext
  import sqlContext.implicits._

  override def process(): DataFrame = {
    dataLoader.load("grc_pp_bv")
      .select($"identifier".as("grc_pp"))
  }

}