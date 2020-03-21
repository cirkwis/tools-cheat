package com.socgen.bsc.cbs.businessviews.grc.delta

import com.socgen.bsc.cbs.businessviews.base.InterceptorFunctions._
import com.socgen.bsc.cbs.businessviews.base.{Interceptor, Settings, StockUpdate}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by X156170 on 31/01/2018.
  */
class BuildGrcPrestaStock(override val dataLoader: DataFrameLoader) extends BusinessView {
  dataLoader.isBvEngineProcess = true
  override val frequency = Frequency.DAILY
  override val name = "grc/prbigdata"
  val deltaName = "grcdelta_prbigdata"

  val configData = ConfigFactory.load("bv_engine/" + deltaName + ".conf")
  val settings = new Settings(configData, deltaName, dataLoader)


  override def process(): DataFrame = {

    val setUp = StockUpdate.loadSettings(settings)
    val linksName = setUp.links.map(l => l.name)

    //TODO : put prepare functions in a file conf to have a cleaner code so much repetitions here
    def checkLink(name: String): Boolean = if (linksName.contains(name)) true else false

    val enrichedHADG05 =
      if (checkLink("grcdelta_HADG05"))
        Some(prepareMapping(Map("ENNOFM_AP" -> Some("ENCOURS")))(15)(dataLoader.load("grcdelta_HADG05".replaceAll("_","/"))))
      else None

    val enrichedHADG07 =
      if (checkLink("grcdelta_HADG07"))
        Some(prepareMapping(Map("ENNOFM_AP" -> Some("ENCOURS")))(15)(dataLoader.load("grcdelta_HADG07".replaceAll("_","/"))))
      else None

    val enrichedHADG09 =
      if (checkLink("grcdelta_HADG09"))
        Some(prepareMapping(Map("ENNOFM_AP" -> Some("ENCOURS")))(15)(dataLoader.load("grcdelta_HADG09".replaceAll("_","/"))))
      else None

    val enrichedHADG06 =
      if (checkLink("grcdelta_HADG06"))
        Some(prepareMapping(Map("ENNOFM_AP" -> Some("ENCOURS")))(15)(dataLoader.load("grcdelta_HADG06".replaceAll("_","/"))))
      else None

    val enrichedHADG16 =
      if (checkLink("grcdelta_HADG16"))
        Some(prepareMapping(Map("ENNOFM_AP" -> Some("ENCOURS")))(15)(dataLoader.load("grcdelta_HADG16".replaceAll("_","/"))))
      else None

    val enrichedHADG14 =
      if (checkLink("grcdelta_HADG14"))
        Some(prepareMapping(Map("MTAURE_AP" -> Some("MTAURE"), "MTAUPA_AP" -> Some("MTAUPA") ))(15)(dataLoader.load("grcdelta_HADG14".replaceAll("_","/"))))
      else None

    val enrichedHADG17 =
      if (checkLink("grcdelta_HADG17"))
        Some(prepareMapping(Map("MTNOVB3_AP" -> Some("MTNOVB3"), "MTNOVB9_AP" -> Some("MTNOVB9")))(15)(dataLoader.load("grcdelta_HADG17".replaceAll("_","/"))))
      else None

    val HADG05Interceptor = Interceptor("grcdelta_HADG05", enrichedHADG05)
    val HADG07Interceptor = Interceptor("grcdelta_HADG07", enrichedHADG07)
    val HADG09Interceptor = Interceptor("grcdelta_HADG09", enrichedHADG09)
    val HADG06Interceptor = Interceptor("grcdelta_HADG06", enrichedHADG06)
    val HADG16Interceptor = Interceptor("grcdelta_HADG16", enrichedHADG16)
    val HADG14Interceptor = Interceptor("grcdelta_HADG14", enrichedHADG14)
    val HADG17Interceptor = Interceptor("grcdelta_HADG17", enrichedHADG17)

    val interceptors = List(
      HADG05Interceptor,
      HADG06Interceptor,
      HADG07Interceptor,
      HADG09Interceptor,
      HADG16Interceptor,
      HADG14Interceptor,
      HADG17Interceptor
    )

    StockUpdate.compute(setUp, Some(interceptors.filter(i => i.enrichedDataSet.isDefined))).filter(col("OWNER_ASSET_NUM").isNotNull)
  }


}
