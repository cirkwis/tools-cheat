package com.socgen.bsc.cbs.businessviews.insee

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, Transformation}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.ExtraFunctionsDF._
import scala.util.matching.Regex

/**
  * Created by X156170
  * This class handles the businessView for insee
  *
  *
  */
class BuildInseeLeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val name = "insee_le_bv"
  override val frequency = Frequency.DAILY
  val enrichedInsee = "insee"

  override def process(): DataFrame = {
    val formerBvInsee = dataLoader.loadWithOption(name)
    val inseeFullWithNewColumns = dataLoader.load(enrichedInsee)
    
      //there is data in
      val defaultBv = inseeFullWithNewColumns.applyOperations(getListOfDefaultTransformation())
      val defaultBvWithoutDuplicates = eliminateDuplicates(defaultBv,"siret").cache()
      if (formerBvInsee == None) {
        //it's the first businessView for insee, then the finalView is the  temporaryView
        defaultBvWithoutDuplicates
      }
      else {
        val YYYYmmDD = getYYYYmmDD(name)
        val date = concatenate("-")(YYYYmmDD)

        //it's not the first, then we enrich data with the historical of the last businessView
        //select of the sirets in the temporaryView
        val siretsOfTemporaryBusinessView = selectColumn(defaultBvWithoutDuplicates, "siret")

        //select of the sirets in the last businessView (D-1)
        val siretsOfFormerBusinessView = selectColumn(formerBvInsee.get, "siret")

        //we check the deleted sirets
        val siretsDeleted = siretsOfFormerBusinessView.except(siretsOfTemporaryBusinessView).select(col("siret").as("siret_"))

        //we got all the informations about the sirets deleted by joining with the last businessView (D-1)
        val thirdPartiesDeleted =
        siretsDeleted.join(formerBvInsee.get, col("siret_").equalTo(col("siret")), "left_outer")
          .applyOperations(getListOfExtraTransformation(), true)
          .select(getFinalColumns().map(col): _*)

        //we got the final view (D) by adding the thirdParties deleted to the temporaryView
        defaultBvWithoutDuplicates.unionAll(thirdPartiesDeleted).filterDuplicates("duplicates_count", Seq("siret"))
      }
  }

  def getYYYYmmDD(datasetName: String): Seq[String] = {
    val pattern = new Regex("([a-z]{4}[=]\\d{4})[/]([a-z]{2}[=]\\d{2})[/]([a-z]{2}[=]\\d{2})")
    val path = dataLoader.findLatestPartition(datasetName).toString()
    val matchingIterator = pattern.findAllMatchIn(path)
    val date = matchingIterator.next().toString().split("/")
    for (i <- 0 to 2) yield date(i).split("=")(1)
  }

  def getListOfDefaultTransformation(): List[Transformation] = {
    List(
      renameCol("insee_hash","insee_hash"),
      mergeFields("siret", concatenate(""), "SIREN", "NIC"),
      mergeFields("headquarters_address", concatenate(" "), "L2_NORMALISEE", "L3_NORMALISEE", "L4_NORMALISEE",
        "L5_NORMALISEE", "L6_NORMALISEE"),
      createStaticColumn("database_deletion_date", ""),
      createStaticColumn("legal_situation", "In activity"),
      renameCol("NJ", "legal_category_code"),
      renameCol("SIGLE", "acronym"),
      renameCol("APEN700", "ape_insee5_code"),
      renameCol("L1_NORMALISEE", "legal_name"),
      renameCol("NOMEN_LONG", "legal_name_90c"),
      renameCol("SIREN", "siren_identifier"),
      renameCol("L7_NORMALISEE", "dest_country"),
      renameCol("DCREN", "company_creation_date"),
      renameCol("SIEGE", "office_type"),
      renameCol("ENSEIGNE", "brand"),
      renameCol("APET700", "establishment_activity"),
      renameCol("ACTIVNAT", "establishment_activity_nature"),
      renameCol("MONOACT", "monoactivity_index"),
      renameCol("CATEGORIE", "business_category"),
      renameCol("TCA", "annual_sales_local"),
      renameCol("EFETCENT", "employees_here"),
      renameCol("EFENCENT", "employees_total")
    )
  }

  def getListOfExtraTransformation(): List[Transformation] = {
    List(
      createStaticColumn("legal_situation", "Activity Discontinued"),
      createStaticColumn("database_deletion_date", concatenate("-")(getYYYYmmDD(name)))
    )
  }

  def selectColumn(df: DataFrame, colName: String): DataFrame = {
    df.select(col(colName))
  }

  def getFinalColumns(): List[String] = {
    List(
      "insee_hash","siret", "headquarters_address", "database_deletion_date", "legal_situation",
      "legal_category_code", "acronym", "ape_insee5_code", "legal_name", "legal_name_90c",
      "siren_identifier", "dest_country", "company_creation_date", "office_type", "brand",
      "establishment_activity", "establishment_activity_nature", "monoactivity_index",
      "business_category", "annual_sales_local", "employees_here", "employees_total"
    )
  }

}

