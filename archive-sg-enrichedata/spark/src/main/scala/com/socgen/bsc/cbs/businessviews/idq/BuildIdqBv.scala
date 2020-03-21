package com.socgen.bsc.cbs.businessviews.idq

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, Transformation}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}


/**
  * Created by X156170 on 21/02/2017.
  */
class BuildIdqBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override def name: String = "idq_le_bv"

  override val frequency = Frequency.DAILY

  override def process(): DataFrame = {
    val dataFrame = dataLoader.load("idq")

    val transformedDf = dataFrame.applyOperations(getListOfTransformations())
    val occurenceCountedRdd = getOccurenceNumberRdd(transformedDf, "duplicate_id")
    val struct = new StructType(Array(StructField("id", StringType), StructField("count", IntegerType)))

    val occurenceCounterDf = occurenceCountedRdd match {
      case Some(x) => dataLoader.sqlContext.createDataFrame(x, struct)
      case None => throw new Exception(s"The column doesn't exist")
    }

    transformedDf.join(occurenceCounterDf, col("duplicate_id").equalTo(col("id")))
      .drop(col("id"))
      .withColumnRenamed("count", "duplicates_number")
      .withColumn("duplicates_number", col("duplicates_number").cast(StringType))
  }

  def getListOfTransformations(): List[Transformation] = {
    List(
      renameCol("IDENTIFIANT_PM", "identifier"),
      renameCol("NOM_LONG", "legal_name_90c"),
      renameCol("CODE_ACTIVITE_REEL", "naer5_code"),
      renameCol("CODE_PAYS_NATIONALITE", "nationality_country_code"),
      applyMaper("siren_identifier", sirenMaper(), "SIREN"),
      mergeFields("headquarters_address", concatenate(" "), "COMPLEMENT_POINT_GEO", "VOIE", "LIEU_DIT", "CODE_POSTAL_LOCALITE"),
      applyMaper("relationship_status", dateIdqMaper(), "DATE_FIN_RELATION"),
      renameCol("CONTROLE_DOUBLONS_EXACTS", "duplicate_id")
    )
  }

  def getOccurenceNumberRdd(df: DataFrame, columnName: String): Option[RDD[Row]] = {
    Try(df(columnName)) match {
      case Success(column) => {
        val df_rdd = df.select(columnName).rdd
          .map(r => r.getString(0))
          .map(x => (x, 1))
          .reduceByKey(_ + _)
          .map(x => Row.fromTuple(x))
        Some(df_rdd)
      }
      case Failure(e) => None
    }
  }

  def dateIdqMaper() = {
    (columnToMap: String) => {
      columnToMap match {
        case "" => "Open"
        case _ => "Closed"
      }
    }
  }

  def sirenMaper() = {
    (columnToMap: String) => {
      columnToMap match {
        case "vide" => null
        case _ => columnToMap
      }
    }
  }
}
