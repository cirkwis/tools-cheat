package com.socgen.bsc.cbs.businessviews.infogreffe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, Transformation}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import scala.collection.mutable.LinkedHashMap


/**
  * Created by X156170 on 12/12/2016
  */
class BuildInfogreffeDeregisteredLeBv(override val dataLoader: DataFrameLoader) extends BusinessView with InfogreffeBv {
  override val frequency = Frequency.MONTHLY
  override val name = "infogreffe_deregistered_le_bv"

  override def process(): DataFrame = {
    val transformedAndNormalizedDataFrames = getTransformedAndNormalizedDataFrames()
    assemblyFinalDataFrame(transformedAndNormalizedDataFrames)
  }

  override def getDataLoader(): DataFrameLoader = this.dataLoader

  override def getDataFramesNames(): List[String] = {
    (for (i <- 2 to 6) yield "infogreffe_entreprises_radiees_201" + i.toString).toList
  }

  override def getTransformationsAndYears(): LinkedHashMap[Transformation, List[String]] = {
    LinkedHashMap(
      renameCol("siren", "siren") -> List("all"),
      renameCol( "date","deregistration_date") -> List("2012", "2013", "2014"),
      renameCol("date_de_radiation","deregistration_date") -> List("2015"),
      renameCol("date_radiation","deregistration_date") -> List("2016"),
      createStaticColumn("legal_situation","Struck off") -> List("all"),
      renameCol("year", "year") -> List("all")
    )
  }

  override def getDataFramesAndYears() = {
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
      "deregistration_date" -> StringType,
      "legal_situation" -> StringType,
      "year" -> IntegerType
    )
  }


}


