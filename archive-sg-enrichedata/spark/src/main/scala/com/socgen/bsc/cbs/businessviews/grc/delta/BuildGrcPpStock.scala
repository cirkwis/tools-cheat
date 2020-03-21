package com.socgen.bsc.cbs.businessviews.grc.delta

import com.socgen.bsc.cbs.businessviews.base.{Interceptor, InterceptorFunctions, Settings, StockUpdate}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class BuildGrcPpStock(override val dataLoader: DataFrameLoader) extends BusinessView {
  import InterceptorFunctions._

  dataLoader.isBvEngineProcess = true
  override val frequency = Frequency.DAILY
  override val name = "grc/ppbigdata"
  val deltaName = "grcdelta_ppbigdata"

  val configData = ConfigFactory.load("bv_engine/" + deltaName + ".conf")
  val settings = new Settings(configData, deltaName, dataLoader)


  override def process(): DataFrame = {

    val setUp = StockUpdate.loadSettings(settings)
    val linksName = setUp.links.map(l => l.name)

    def checkLink(name: String): Boolean = if (linksName.contains(name)) true else false

/*
    val enrichedHADT01 = if (checkLink("grcdelta_HADT01"))
      Some(prepare("grcdelta_HADT01")(dataLoader))
    else None
*/

    val enrichedHADT03 = if (checkLink("grcdelta_HADT03"))
      Some(prepareMapping(Map("ANNL_REVENUE" -> None))(17)(dataLoader.load("grcdelta_HADT03".replaceAll("_","/"))))
    else None

    val enrichedHADT07 =
      if (checkLink("grcdelta_HADT07")) Some(prepareMonacoBddf(dataLoader.sqlContext)(dataLoader.load("grcdelta_HADT07".replaceAll("_","/")))("ID_PP"))
      else None

/*
    val hadt01Interceptor = Interceptor("grcdelta_HADT01", enrichedHADT01)
*/
    val hadt03Interceptor = Interceptor("grcdelta_HADT03", enrichedHADT03)
    val hadt07Interceptor = Interceptor("grcdelta_HADT07", enrichedHADT07)

    val interceptors = List(hadt03Interceptor, hadt07Interceptor)

    StockUpdate.compute(setUp, Some(interceptors.filter(i => i.enrichedDataSet.isDefined))).filter(col("PERSON_UID").isNotNull)
  }
}
