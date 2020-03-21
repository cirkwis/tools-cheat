package com.socgen.bsc.cbs.businessviews.infogreffe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import scala.collection.mutable

/**
  * Created by Guy Stephane Manganneau on 12/12/2016.
  */
class BuildInfogreffeKeyfiguresBv(override val dataLoader: DataFrameLoader) extends BusinessView with InfogreffeBv {
  override val frequency = Frequency.MONTHLY
  override val name = "infogreffe_chiffres_cles_bv"


  override def process(): DataFrame = {

    val transformedAndNormalizedDataFrames = getTransformedAndNormalizedDataFrames()
    assemblyFinalDataFrame(transformedAndNormalizedDataFrames)
  }


  override def getDataLoader(): DataFrameLoader = this.dataLoader

  override def getDataFramesNames(): List[String] = {
    List(
      "infogreffe_chiffres_cles_2011_to_2013",
      "infogreffe_chiffres_cles_2012_to_2014",
      "infogreffe_chiffres_cles_2015",
      "infogreffe_chiffres_cles_2016"
    )
  }

  override def getDataFramesAndYears(): Map[Int, String] = {
    Map(
      0 -> "2013",
      1 -> "2014",
      2 -> "2015",
      3 -> "2016"
    )
  }

  override def getTransformationsAndYears() = {
    mutable.LinkedHashMap(
      renameCol("siren", "siren") -> List("all"),
      renameCol("ca_2011", "as_2011") -> List("2013"),
      renameCol("ca_2012", "as_2012") -> List("2013", "2014"),
      renameCol("ca_2013", "as_2013") -> List("2013", "2014"),
      applyFilter("as_2013", customFilter(List("2013"), true), "ca_3", "millesime_3") -> List("2015"),
      renameCol("ca_2014", "as_2014") -> List("2014"),
      applyFilter("as_2014", customFilter(List("2014"), true), "ca_2", "millesime_2") -> List("2015"),
      applyFilter("as_2014", customFilter(List("2014"), true), "ca_3", "millesime_3") -> List("2016"),
      applyFilter("as_2015", customFilter(List("2015"), true), "ca_1", "millesime_1") -> List("2015"),
      applyFilter("as_2015", customFilter(List("2015"), true), "ca_2", "millesime_2") -> List("2016"),
      applyFilter("as_2016", customFilter(List("2016"), true), "ca_1", "millesime_1") -> List("2016"),
      extract("as_bracket_2011", extractIndex(1), "tranche_ca_2011") -> List("2013"),
      extract("as_bracket_2012", extractIndex(1), "tranche_ca_2012") -> List("2013", "2014"),
      applyFilter("as_bracket_2016", customFilterAndExtract(List("2016"), true, 1), "tranche_ca_millesime_1", "millesime_1") -> List("2016"),
      renameCol("resultat_2011", "profit_2011") -> List("2013"),
      renameCol("resultat_2012", "profit_2012") -> List("2013", "2014"),
      renameCol("resultat_2013", "profit_2013") -> List("2013", "2014"),
      applyFilter("profit_2013", customFilter(List("2013"), true), "resultat_3", "millesime_3") -> List("2015"),
      renameCol("resultat_2014", "profit_2014") -> List("2014"),
      applyFilter("profit_2014", customFilter(List("2014"), true), "resultat_2", "millesime_2") -> List("2015"),
      applyFilter("profit_2014", customFilter(List("2014"), true), "resultat_3", "millesime_3") -> List("2016"),
      applyFilter("profit_2015", customFilter(List("2015"), true), "resultat_1", "millesime_1") -> List("2015"),
      applyFilter("profit_2015", customFilter(List("2015"), true), "resultat_2", "millesime_2") -> List("2016"),
      applyFilter("profit_2016", customFilter(List("2016"), true), "resultat_1", "millesime_1") -> List("2016"),
      renameCol("effectifs_2011", "workforce_2011") -> List("2013"),
      renameCol("effectifs_2012", "workforce_2012") -> List("2013", "2014"),
      renameCol("effectifs_2013", "workforce_2013") -> List("2013", "2014"),
      applyFilter("workforce_2013", customFilter(List("2013"), true), "effectif_3", "millesime_3") -> List("2015"),
      renameCol("effectifs_2014", "workforce_2014") -> List("2014"),
      applyFilter("workforce_2014", customFilter(List("2014"), true), "effectif_2", "millesime_2") -> List("2015"),
      applyFilter("workforce_2014", customFilter(List("2014"), true), "effectif_3", "millesime_3") -> List("2016"),
      applyFilter("workforce_2015", customFilter(List("2015"), true), "effectif_1", "millesime_1") -> List("2015"),
      applyFilter("workforce_2015", customFilter(List("2015"), true), "effectif_2", "millesime_2") -> List("2016"),
      applyFilter("workforce_2016", customFilter(List("2016"), true), "effectif_1", "millesime_1") -> List("2016"),
      castColumn("as_2013","Integer")->List("2013"),
      applyMaper("as_bracket_2013", intervalMaper(), "as_2013") -> List("2013"),
      extract("as_bracket_2014", extractIndex(1), "tranche_ca_2014") -> List("2014"),
      castColumn("as_2014","Integer")->List("2016"),
      castColumn("as_2015","Integer")->List("2016"),
      applyMaper("as_bracket_2014", intervalMaper(), "as_2014") -> List("2016"),
      applyMaper("as_bracket_2015", intervalMaper(), "as_2015") -> List("2016"),
      renameCol("year", "year") -> List("all")
    )
  }

  override def getTmpOutputStructure(): mutable.LinkedHashMap[String, DataType] = {
    mutable.LinkedHashMap(
      "siren" -> StringType,
      "as_2011" -> StringType,
      "as_2012" -> StringType,
      "as_2013" -> StringType,
      "as_2014" -> StringType,
      "as_2015" -> StringType,
      "as_2016" -> StringType,
      "as_bracket_2011" -> StringType,
      "as_bracket_2012" -> StringType,
      "as_bracket_2013" -> StringType,
      "as_bracket_2014" -> StringType,
      "as_bracket_2015" -> StringType,
      "as_bracket_2016" -> StringType,
      "profit_2011" -> StringType,
      "profit_2012" -> StringType,
      "profit_2013" -> StringType,
      "profit_2014" -> StringType,
      "profit_2015" -> StringType,
      "profit_2016" -> StringType,
      "workforce_2011" -> StringType,
      "workforce_2012" -> StringType,
      "workforce_2013" -> StringType,
      "workforce_2014" -> StringType,
      "workforce_2015" -> StringType,
      "workforce_2016" -> StringType,
      "year" -> IntegerType
    )
  }

}


