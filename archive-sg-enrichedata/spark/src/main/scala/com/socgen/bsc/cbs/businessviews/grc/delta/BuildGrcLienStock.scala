package com.socgen.bsc.cbs.businessviews.grc.delta

import com.socgen.bsc.cbs.businessviews.base.{Settings, StockUpdate}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
  * Created by X156170 on 25/01/2018.
  */
class BuildGrcLienStock (override val dataLoader: DataFrameLoader) extends BusinessView {
  dataLoader.isBvEngineProcess = true
  override val frequency = Frequency.DAILY
  override val name = "grc/ttbigdata"
  val deltaName = "grcdelta_ttbigdata"

  val configData = ConfigFactory.load("bv_engine/" + deltaName + ".conf")
  val settings  = new Settings(configData, deltaName, dataLoader)


  override def process(): DataFrame = {

    val setUp = StockUpdate.loadSettings(settings)
    StockUpdate.compute(setUp, None).filter(col("UID_TIERS").isNotNull || col("UID_TIERS_LIE").isNotNull)
  }


}
