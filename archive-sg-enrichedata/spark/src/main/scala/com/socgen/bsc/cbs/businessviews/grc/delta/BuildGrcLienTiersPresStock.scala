package com.socgen.bsc.cbs.businessviews.grc.delta

import com.socgen.bsc.cbs.businessviews.base.{Settings, StockUpdate}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by X156170 on 01/02/2018.
  */
class BuildGrcLienTiersPresStock(override val dataLoader: DataFrameLoader) extends BusinessView {
  dataLoader.isBvEngineProcess = true
  override val frequency = Frequency.DAILY
  override val name = "grc/libigdata"
  val deltaName = "grcdelta_libigdata"

  val configData = ConfigFactory.load("bv_engine/" + deltaName + ".conf")
  val settings = new Settings(configData, deltaName, dataLoader)

  override def process(): DataFrame = {
    val setUp = StockUpdate.loadSettings(settings)
    StockUpdate.compute(setUp, None).filter(col("PARTY_UID").isNotNull || col("OWNER_ASSET_NUM").isNotNull)
  }


  }
