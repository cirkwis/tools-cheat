package com.socgen.bsc.cbs.businessviews.infogreffe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, Transformation}

import scala.collection.mutable.LinkedHashMap

/**
  * Created by Guy Stephane Manganneau on 12/12/2016.
  */

class BuildInfogreffeRegisteredLeBv(override val dataLoader: DataFrameLoader) extends BusinessView with InfogreffeBv {
  override val frequency = Frequency.MONTHLY
  override val name = "infogreffe_registered_le_bv"

  override def process(): DataFrame = {
    val transformedAndNormalizedDataFrames = getTransformedAndNormalizedDataFrames()
    assemblyFinalDataFrame(transformedAndNormalizedDataFrames)
  }

  override def getDataLoader(): DataFrameLoader = this.dataLoader

  override def getDataFramesNames(): List[String] = {
    (for (i <- 2 to 6) yield "infogreffe_entreprises_immatriculees_201" + i.toString).toList
  }

  override def getDataFramesAndYears(): Map[Int, String] = {
    Map(
      0 -> "2012",
      1 -> "2013",
      2 -> "2014",
      3 -> "2015",
      4 -> "2016"
    )
  }

  override def getTmpOutputStructure(): LinkedHashMap[String, DataType] = {
    LinkedHashMap(
      "siren" -> StringType,
      "siret" -> StringType,
      "ape_code" -> StringType,
      "ape_name" -> StringType,
      "business_listing" -> StringType,
      "city" -> StringType,
      "company_creation_date" -> StringType,
      "legal_form" -> StringType,
      "legal_form_code" -> StringType,
      "legal_name_90c" -> StringType,
      "legal_situation" -> StringType,
      "region" -> StringType,
      "registry_name" -> StringType,
      "siren" -> StringType,
      "headquarters_address" -> StringType,
      "year" -> IntegerType
    )
  }

  override def getTransformationsAndYears(): LinkedHashMap[Transformation, List[String]] = {
    LinkedHashMap(
      renameCol("siren", "siren") -> List("all"),
      mergeFields("siret", concatenate(""), "siren", "nic") -> List("2016"),
      renameCol("code_ape", "ape_code") -> List("all"),
      renameCol("libelle", "ape_name") -> List("2012", "2013", "2014", "2015"),
      renameCol("libelle_ape", "ape_name") -> List("2016"),
      renameCol("business_listing", "fiche_identite") -> List("all"),
      renameCol("ville", "city") -> List("all"),
      renameCol("date", "company_creation_date") -> List("2012", "2013", "2014"),
      renameCol("date_d_immatriculation", "company_creation_date") -> List("2015"),
      renameCol("date_immatriculation", "company_creation_date") -> List("2016"),
      createStaticColumn("legal_situation","Active") -> List("all"),
      renameCol( "forme_juridique","legal_form") -> List("all"),
      renameCol( "forme_juridique_code","legal_form_code") -> List("2015"),
      renameCol( "denomination","legal_name_90c") -> List("all"),
      renameCol("region", "region") -> List("all"),
      renameCol( "greffe","registry_name") -> List("all"),
      mergeFields("headquarters_address", concatenate(" "), "adresse", "code_postal", "city") -> List("all"),
      renameCol("year", "year") -> List("all")
    )
  }

}
