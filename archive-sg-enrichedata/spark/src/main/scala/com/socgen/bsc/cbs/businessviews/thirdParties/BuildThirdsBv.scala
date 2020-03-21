package com.socgen.bsc.cbs.businessviews.thirdParties

import com.socgen.bsc.cbs.businessviews.common.{DataFrameLoader, Frequency, MultipleViewsAndOutputFormats}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import com.socgen.bsc.cbs.businessviews.thirdParties.config.DataSourceName
import com.socgen.bsc.cbs.businessviews.thirdParties.config.DataSourceName.{apply => _, _}
import com.socgen.bsc.cbs.businessviews.thirdParties.config.ThirdPartiesBvConfig._
import com.socgen.bsc.cbs.businessviews.thirdParties.models.ThirdType._
import com.socgen.bsc.cbs.businessviews.thirdParties.models.{ContainerResult, ThirdParties}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


class BuildThirdsBv(override val dataLoader: DataFrameLoader, thirdType: ThirdType = PM) extends MultipleViewsAndOutputFormats {
  override val frequency = Frequency.DAILY

  val sqlContext = dataLoader.sqlContext

  import sqlContext.implicits._
  import scala.collection.JavaConversions._

  var thirdPartyIDsCached = dataLoader.loadAll(outputMappingViewName(thirdType)) match {
    case Some(uids) => {
      uids.cache()
      Some(uids)
    }
    case None => None
  }

  val matchedThirdParties: DataFrame = dataLoader.load(globalMatchedBvFor(thirdType))

  val config = ConfigFactory.load("environment.conf")
  val thirdsConfigPath = s"env.$thirdType"
  var avroSchemaPath = config.getString(s"$thirdsConfigPath.avroSchemaPath")
  var avroSchemaFiles = config.getStringList(s"$thirdsConfigPath.avroSchemas").toList
  var usedDataSources: List[DataSourceName] = config.getStringList(s"$thirdsConfigPath.dataSources").map(DataSourceName.withName(_)).toList

  override def process(): Map[String, (DataFrame, String)] = {
    // 1- Enrich the matched Third Parties by UIDs from the third party IDs mapping table.
    val enrichedDFByUIDs = ThirdParties.enrichByUIDs(matchedThirdParties, usedDataSources, thirdPartyIDsCached) match {
      case Some(enrichedDF) => enrichedDF
      case None => matchedThirdParties
    }

    // 2- Enrich the matched Third Parties by the whole DataSources.
    val enrichedDFByDataSets = ThirdParties.enrichByDataSources(avroSchemaPath, avroSchemaFiles, usedDataSources, thirdType, enrichedDFByUIDs, dataLoader)

    // 3- Get a list of third parties as a RDD[ThirdPartyObject]
    //TODO - TP: Detect the broken links: GroupBy, and calculate a new UUID. Case 2
    val thirdPartyObjects: RDD[ContainerResult] = ThirdParties.compute(avroSchemaPath, avroSchemaFiles, usedDataSources, enrichedDFByDataSets)

    // 4- Normalize the third Parties fields as specified on Avro schemas.
    val thirdPartiesRDD: RDD[Row] = thirdPartyObjects.map(_.row)
    val structType: StructType = AvroFunctions.getStructTypeFromAvroFile(avroSchemaPath, (avroSchemaFiles): _*)
    val thirdPartiesDF: DataFrame = sqlContext.createDataFrame(thirdPartiesRDD, structType)

    // 5- Collect the new third parties IDs
    val newThirdPartyIDs = thirdPartyObjects.flatMap(_.thirdPartyIDs).toDF()

    val outputJson = ConfigFactory.load("environment.conf").getBoolean("env.jsonOutputBuildThird")

    if (outputJson)
      save(thirdPartiesDF.repartition(1), outputViewName(thirdType) + "_json", "json")

    Map[String, (DataFrame, String)](
      outputViewName(thirdType) -> (thirdPartiesDF, "com.databricks.spark.avro"),
      outputMappingViewName(thirdType) -> (newThirdPartyIDs, "parquet"))
  }

}