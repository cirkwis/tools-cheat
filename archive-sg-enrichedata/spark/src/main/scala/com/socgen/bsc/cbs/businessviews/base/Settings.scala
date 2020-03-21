package com.socgen.bsc.cbs.businessviews.base

import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try


/**
  * Created by X153279 on 15/01/2018.
  */


//TODO add comments later
case class Settings(private val config: com.typesafe.config.Config, name: String, dataloader: DataFrameLoader) {
  private val P_PACKAGE = "sg.cbs." + name

  val baseSettings = BaseSettings(config.getConfig(s"$P_PACKAGE" + ".base"), dataloader)
  val linksSettings = LinksSettings(config.getConfig(s"$P_PACKAGE"), dataloader)
  val columnsSettings = ColumnsSettings(config.getConfig(s"$P_PACKAGE"), linksSettings)
}

case class BaseSettings(private val config: com.typesafe.config.Config, dataloader: DataFrameLoader) {

  val base: Base = new Base(dataloader.load(config.getString("name").replaceAll("_","/")), config.getString("name"), config.getStringList("pk").asScala.map(_.toString).toList)

}

case class ColumnsSettings(private val config: com.typesafe.config.Config, linksSettings: LinksSettings) {
  val configList = config.getObjectList("columns").asScala


  val dfList = linksSettings.links.map(_.name)


  val columns: List[Column] = configList.map({
    conf => new Column(conf.toConfig.getString("name"), potentielValues(conf.toConfig), mappingColum(conf.toConfig),defaultValues(conf.toConfig))
  }).toList


  def mappingColum(config: Config): Option[String] = {
    val column = Try(config.getString("mapping_with").replace(".", "_")).toOption
    if (column != None && !(dfList.filter(column.get.contains(_)).isEmpty)) Some(column.get)
    else None
  }


  def potentielValues(conf: Config): Option[List[PotentialValue]] = {
    val objetList = Try(conf.getObjectList("potential_values").asScala).toOption

    def isSchemaExist(potentialValue: PotentialValue): Boolean = !(dfList.filter(potentialValue.value.contains(_)).isEmpty)
    val pValues: Option[List[PotentialValue]] = objetList match {
      case None => None
      case Some(o) => val potentielValues = o.map({ conf =>
        PotentialValue(
          conf.toConfig.getString("value").replace(".", "_"),
          SqlLike(conf.toConfig.getString("condition").replace(".","_"),
            conf.toConfig.getString("value").replace(".", "_"))
        )
      }).filter(isSchemaExist(_)).toList
        if (potentielValues.isEmpty) None else Some(potentielValues)
    }
    pValues
  }

  def defaultValues (config : Config) : Option[String] = {
    val defaultValue = Try(config.getString("default_value")).toOption
    if (defaultValue != None) Some(defaultValue.get) else None
  }
}

case class LinksSettings(private val config: com.typesafe.config.Config, dataLoader: DataFrameLoader) {
  val configList = config.getObjectList("links").asScala
  val links: List[Link] = configList.map({ conf =>
    new Link(
      dataLoader.loadWithOption(conf.toConfig.getString("name").replaceAll("_","/")),
      conf.toConfig.getString("name"),
      conf.toConfig.getStringList("pk").asScala.toList)
  }).filter(_.df.isDefined).toList
}

