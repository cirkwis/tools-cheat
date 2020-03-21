package com.socgen.bsc.cbs.businessviews.rct

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.common.BusinessView
import com.socgen.bsc.cbs.businessviews.common.Frequency

class BuildIdmutBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "rct_idmut_bv"

  override def process(): DataFrame = {
    val sqlContext = dataLoader.sqlContext
    import sqlContext.implicits._

    dataLoader.load("rct_spmidmut")
      .select($"SPMIDMUT_NUPERS".as("idmut_le_identifier"),
        $"SPMIDMUT_TYMULTI".as("idmut_tymulti"),
        $"SPMIDMUT_VAMULTI".as("idmut_vamulti"))
  }
}