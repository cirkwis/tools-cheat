package com.socgen.bsc.cbs.businessviews.grc.delta

import com.socgen.bsc.cbs.businessviews.base.InterceptorFunctions._
import com.socgen.bsc.cbs.businessviews.base.{Interceptor, Settings, StockUpdate}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._




/**
  * Created by X156170 on 25/01/2018.
  */
class BuildGrcPmStock(override val dataLoader: DataFrameLoader) extends BusinessView {
  dataLoader.isBvEngineProcess = true
  override val frequency = Frequency.DAILY
  override val name = "grc/pmbigdata"
  val deltaName = "grcdelta_pmbigdata"

  val configData = ConfigFactory.load("bv_engine/" + deltaName + ".conf")
  val settings = new Settings(configData, deltaName, dataLoader)


  override def process(): DataFrame = {

    val setUp = StockUpdate.loadSettings(settings)
    val linksName = setUp.links.map(l => l.name)

    def checkLink(name: String): Boolean = if (linksName.contains(name)) true else false


/*
    val enrichedHADT08 =
      if (checkLink("grcdelta_HADT08")) Some(prepare("grcdelta_HADT08")(dataLoader))
      else None

    val enrichedHADT09 =
      if (checkLink("grcdelta_HADT09")) Some(prepare("grcdelta_HADT09")(dataLoader))
      else None
*/

    val enrichedHADT18 =
      if (checkLink("grcdelta_HADT18")) Some(prepareMapping(Map("ATTRIB_15"->None ,"ATTRIB_24" -> None))(17)(dataLoader.load("grcdelta_HADT18".replaceAll("_","/"))))
      else None

    val enrichedHADT37 =
      if (checkLink("grcdelta_HADT37")) Some(prepareHADT37(dataLoader.sqlContext)(dataLoader.load("grcdelta_HADT37".replaceAll("_","/"))))
      else None

    val enrichedHADT38 =
      if (checkLink("grcdelta_HADT38")) Some(prepareMonacoBddf(dataLoader.sqlContext)(dataLoader.load("grcdelta_HADT38".replaceAll("_","/")))("ID_PM"))
      else None


/*
    val hadt08Interceptor = Interceptor("grcdelta_HADT08", enrichedHADT08)
    val hadt09Interceptor = Interceptor("grcdelta_HADT09", enrichedHADT09)
*/
    val hadt18Interceptor = Interceptor("grcdelta_HADT18", enrichedHADT18)
    val hadt37Interceptor = Interceptor("grcdelta_HADT37", enrichedHADT37)
    val hadt38Interceptor = Interceptor("grcdelta_HADT38", enrichedHADT38)

    val interceptors = List(hadt38Interceptor,hadt37Interceptor,hadt18Interceptor)
    StockUpdate.compute(setUp, Some(interceptors.filter(i => i.enrichedDataSet.isDefined))).filter(col("NAME").isNotNull)
  }


}
