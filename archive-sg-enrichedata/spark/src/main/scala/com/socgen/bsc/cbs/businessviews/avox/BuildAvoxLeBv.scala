package com.socgen.bsc.cbs.businessviews.avox

import com.socgen.bsc.cbs.businessviews.avox.Utils._
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by X149386 on 03/11/2016.
  */


class BuildAvoxLeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency: Frequency.Value = Frequency.DAILY
  val sqlContext = dataLoader.sqlContext
  override val name = "avox_le_bv"
  val schemaAvroPath = "avro"

  override def process(): DataFrame = {

    val headQuartersAddressFields = List("REGISTERED_FLOOR", "REGISTERED_BUILDING", "REGISTERED_STREET_1",
      "REGISTERED_STREET_2", "REGISTERED_STREET_3", "REGISTERED_PO_BOX", "REGISTERED_POST_CODE", "REGISTERED_CITY",
      "REGISTERED_STATE")

    val operationalAddressFields = List("OPERATIONAL_FLOOR", "OPERATIONAL_BUILDING", "OPERATIONAL_STREET_1",
      "OPERATIONAL_STREET_2", "OPERATIONAL_STREET_3", "OPERATIONAL_PO_BOX", "OPERATIONAL_POST_CODE", "OPERATIONAL_CITY",
      "OPERATIONAL_STATE")

    val previousNameFields: Seq[String] = for (i <- 1 to 11) yield "previous_name_" + i
    val tradeAsNameFields: Seq[String] = for (i <- 1 to 11) yield "trade_as_name_" + i

    val transformations = List(
      renameCol("AVID", "avid"),
      renameCol("AVOX_ENTITY_CLASS", "client_type"),
      renameCol("COMPANY_WEBSITE", "company_website"),
      renameCol("DATE_OF_DISSOLUTION", "date_of_dissolution"),
      renameCol("REGISTERED_COUNTRY", "dest_country"),
      renameCol("ENTITY_TYPE", "entity_type"),
      mergeFields("headquarters_address", concatenate(" "), headQuartersAddressFields: _*),
      renameCol("CLIENT_RECORD_ID", "identifier"),
      renameCol("IMMEDIATE_PARENT_AVID", "immediate_parent_avid"),
      renameCol("IMMEDIATE_PARENT_COUNTRY", "immediate_parent_country"),
      renameCol("IMMEDIATE_PARENT_NAME", "immediate_parent_name"),
      renameCol("LEGAL_FORM", "legal_form"),
      renameCol("LEGAL_NAME", "legal_name_90c"),
      renameCol("TRADING_STATUS", "legal_situation_code"),
      renameCol("LEI", "lei_id"),
      renameCol("LEI_LOU_ID", "lei_lou_id"),
      renameCol("LOU_STATUS", "lou_status"),
      renameCol("NACE_CODE", "nace_code"),
      applyMaper("nace2_code", formatCodeNace , "NACE2_CODE"),
      applyMaper("nace2_code_2", extract2FirstChar, "nace2_code"),
      renameCol("NAICS_CODE", "naics_2002_code"),
      renameCol("NAICS12_CODE", "naics_2012_code"),
      applyMaper("section", getSectionCode, "nace2_code_2"),
      applyMaper("division", getDivisionCode, "nace2_code"),
      applyMaper("groupe", getGroupCode, "nace2_code"),
      applyMaper("classe", getClassCode, "nace2_code"),
      applyMaper("sous_classe", getSousClassCode, "nace2_code"),
      mergeFields("operational_address", concatenate(" "), operationalAddressFields: _*),
      applyFilter("other_national_id", customFilter(List("FR"), false), "REGISTRATION_NUMBER", "dest_country"),
      splitFields(previousNameFields, split("\\|", 11), "PREVIOUS_NAME"),
      renameCol("REGISTERED_AGENT_NAME", "registered_agent_name"),
      renameCol("REGULATED_BY", "regulated_by"),
      renameCol("REGULATORY_ID", "regulatory_id"),
      renameCol("US_SIC_CODE", "sic_code_1987"),
      renameCol("US_SIC_DESCRIPTION", "sic_descr_1987"),
      applyFilter("siren_identifier", customFilter(List("FR"), true), "REGISTRATION_NUMBER", "dest_country"),
      renameCol("SWIFT_BIC", "swift_number"),
      renameCol("TAX_ID", "tax_id"),
      splitFields(tradeAsNameFields, split("\\|", 11), "TRADES_AS_NAME"),
      renameCol("ULTIMATE_PARENT_AVID", "ultimate_parent_avid"),
      renameCol("ULTIMATE_PARENT_COUNTRY", "ultimate_parent_country"),
      renameCol("ULTIMATE_PARENT_NAME", "ultimate_parent_name"),
      renameCol("REGISTERED_CITY", "city")
    )
    val avoxDf = dataLoader.load("avox_full").applyOperations(transformations)



    val naceRev2Df = createNaceRev2(avoxDf)

    avoxDf.join(naceRev2Df, naceRev2Df.col("id") === avoxDf.col("identifier"), "left_outer")
      .drop("nace2_code")
      .drop("nace2_code_2")
      .drop("section")
      .drop("groupe")
      .drop("division")
      .drop("classe")
      .drop("sous_classe")
      .filterDuplicates("duplicates_count", Seq("identifier"))

  }

  def createNaceRev2(df: DataFrame): DataFrame = {
    val naceRev2Schema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/naceRev2.avsc")

    val naceRev2Type: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("nace_rev_2", naceRev2Schema, true)))

    val naceRev2RddRow =
      df.select("identifier", "section", "division", "groupe", "classe", "sous_classe")
        .rdd
        .map {
          row => Row(row.getAs[String]("identifier"), Row(naceRev2Schema.fieldNames.map(row.getAs[String](_)): _*))
        }
    sqlContext.createDataFrame(naceRev2RddRow, naceRev2Type)
  }
}