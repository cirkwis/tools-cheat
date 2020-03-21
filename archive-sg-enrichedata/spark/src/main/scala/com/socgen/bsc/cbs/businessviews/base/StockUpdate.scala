package com.socgen.bsc.cbs.businessviews.base

import org.apache.spark.sql.DataFrame

/**
  * Created by X153279 on 11/01/2018.
  */

case class Interceptor(dataSetName: String,  enrichedDataSet: Option[DataFrame])
case class SetUp(base : Base, links :List[Link], columns : List[Column])
object StockUpdate {

  def compute(setup : SetUp , interceptors: Option[List[Interceptor]]): DataFrame = {
      setup.base.compute(setup.links,interceptors,setup.columns)
  }

  def loadSettings(settings: Settings): SetUp = {
    SetUp(settings.baseSettings.base, settings.linksSettings.links, settings.columnsSettings.columns)
  }
}

