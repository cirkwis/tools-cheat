package com.socgen.bsc.cbs.businessviews.privalia

import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer.{renameCol, _}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class BuildPrivaliaPpBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override def name = "privalia_pp_bv"

  override def frequency = Frequency.DAILY
  var schemaAvroPath = "avro"

  val sqlContext = dataLoader.sqlContext

  /**
    * Function: SelectFromSource
    * Description: Before realizing join task, we need to pr-select just used columns
    *
    * @return a map of [String, DataFrame] so each element is a pair of dataframe and its name (pp, pm, lien, cav, infogrp, grp).
    **/
  def selectFromSource(): Map[String, DataFrame] = {
    var _mapSource: Map[String, DataFrame] = Map()

    val cercles = dataLoader.load("privalia_cercles")
    _mapSource = _mapSource + ("prv_cercles" -> cercles)

    val tiers = dataLoader.load("privalia_tiers").where("TYPERS = 'PP'")
    _mapSource = _mapSource + ("prv_tiers" -> tiers)

    val tiers_pm_pp = dataLoader.load("privalia_tiers_pm_pp")
    _mapSource = _mapSource + ("prv_tiers_pm_pp" -> tiers_pm_pp)

    _mapSource
  }


  /**
    * Construct array of cercle struct
    * @param cercleDf
    * @return
    */

  def groupingCercles(cercleDf  : DataFrame): DataFrame = {
    val simpleCerclesSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/cercles.avsc")
    // StructType for Cav array
    val prvCerclesType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("ccl", ArrayType(simpleCerclesSchema), true)))

    val transformationsCercles = List(
      renameCol("DATCER", "date_creation_cercle"),
      renameCol("NUMGRC", "grc_identifier"),
      renameCol("NUMCER", "num_cercle"),
      renameCol("NUPERS", "rct_identifier"),
      renameCol("ROLCER", "role_tiers_cercle_code"),
      renameCol("LIBROLE", "role_tiers_cercle_name"),
      renameCol("TOPREF", "top_referent"),
      renameCol("TYPCER", "type_cercle")
    )

    val cercles = cercleDf.applyOperations(transformationsCercles)
    // Grouping data by identifier
    val rddRow =
      cercles.rdd
        .filter(_.getAs[String]("rct_identifier") != null)
        .groupBy(r => r.getAs[String]("rct_identifier"))
        .map {
          case (id: String, rows: Seq[Row]) => {
            val nrs = rows.map {
              case row => Row(simpleCerclesSchema.fieldNames.map(row.getAs[String](_)): _*)
            }
            Row(id, nrs)
          }
        }
    sqlContext.createDataFrame(rddRow, prvCerclesType)

  }

  /**
    * Grouping NaceRevDetail
    * @param tiersDf
    * @return
    */
  def groupingNaceRevDetail(tiersDf  : DataFrame): DataFrame = {
    val naceDetailSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/naceRev2.avsc")
    // StructType for Cav array
    val prvCerclesType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("nace_rev_2_detail", naceDetailSchema, true))
    )

    val transformationsNace = List(
      renameCol("NUPERS","id"),
      renameCol("ACTCLS","classe"),
      renameCol("ACTDIV","division"),
      renameCol("ACTGRP","groupe"),
      renameCol("ACTSECT","section"),
      renameCol("ACTSCLS","sous_classe")
    )

    val tiers = tiersDf.applyOperations(transformationsNace)
    // Grouping data by identifier
    val rddRow =
      tiers.rdd
        .map {
          row => Row( row.getAs[String]("id") ,  Row(naceDetailSchema.fieldNames.map(row.getAs[String](_)): _*))
        }
    sqlContext.createDataFrame(rddRow, prvCerclesType)

  }


  /**
    * Construt array of PM
    * @param pmPpDf
    * @param tiersDf
    * @return
    */
  def groupingPm(pmPpDf  : DataFrame, tiersDf : DataFrame): DataFrame = {
    val simpleppSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/pm.avsc")
    // StructType for Cav array
    val prvPpType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("pm", ArrayType(simpleppSchema), true)))

    val transformationsPm = List(
      renameCol("SOCCAP","type_de_lien_economique"),
      renameCol("NADROI","nature_droits"),
      renameCol("PARCAP","pourc_detention"),
      renameCol("NUPE_PP","rct_identifier"),
      renameCol("STATUT_PM","statut_tiers_pm"),
      renameCol("TYPERS_PM","type_tiers_pm"),
      renameCol("NUMG_PM","identifier"),
      createStaticColumn("id", null),
      createStaticColumn("legal_name_90c", null),
      createStaticColumn("type_de_lien", null)
    )

    val pm = pmPpDf.join(tiersDf, pmPpDf.col("NUPE_PP") === tiersDf.col("NUPERS"),"left_outer")
             .where("TYPERS_PM = 'PM'").applyOperations(transformationsPm)
    // Grouping data by identifier
    val rddRow =
      pm.rdd
        .filter(_.getAs[String]("rct_identifier") != null)
        .groupBy(r => r.getAs[String]("rct_identifier"))
        .map {
          case (id: String, rows: Seq[Row]) => {
            val nrs = rows.map {
              case row => Row(simpleppSchema.fieldNames.map(row.getAs[String](_)): _*)
            }
            Row(id, nrs)
          }
        }
    sqlContext.createDataFrame(rddRow, prvPpType)

  }



  override def process() = {

    val transformations = List(
      renameCol("TYPERS","client_type"),
      renameCol("STATCOMPDA","compliance_status"),
      renameCol("SEGCUS","customer_segment"),
      renameCol("DATCON","date_contact"),
      renameCol("TOTENG","engagements_level"),
      renameCol("TOTREVGLOB","global_revenu"),
      renameCol("NUMGRC","identifier"),
      renameCol("MIFCOD","mif_profil_code"),
      renameCol("LIBMIF","mif_profil_name"),
      renameCol("MIFLIB","mif_skill"),
      renameCol("TOTFORT","patrimoine_level"),
      renameCol("NUMPORT","phone_mobile"),
      renameCol("TOPPPE","politically_exposed"),
      renameCol("PPECOD","ppe_code"),
      renameCol("PPELIB","ppe_name"),
      renameCol("DATENDREL","relationship_end_date"),
      renameCol("DATDEBREL","relationship_start_date"),
      renameCol("STATUT","relationship_type_status"),
      renameCol("RISKCOD","risk_code"),
      renameCol("TOPCOMPRISK","top_risk_complete"),
      renameCol("RISKLIB","risk_label"),
      renameCol("TOPEER","top_conformity_eer"),
      renameCol("TOPGIP","topgip"),
      renameCol("TOTAV","total_aum"),
      renameCol("TYPCON","type_contact_code"),
      renameCol("LIBTYPCNT","type_contact_name")
    )


    val _mapSource = selectFromSource()
    val prv_cercles = _mapSource("prv_cercles")
    val prv_tiers = _mapSource("prv_tiers")
    val prv_tiers_pm_pp = _mapSource("prv_tiers_pm_pp")

    val pm = groupingPm(prv_tiers_pm_pp, prv_tiers)
    val cercles = groupingCercles(prv_cercles)
    val naceDetail = groupingNaceRevDetail(prv_tiers)

    val tiers = prv_tiers.applyOperations(transformations)

    tiers
      .join(pm, tiers.col("identifier") === pm.col("id"),"left_outer")
      .join(cercles, tiers.col("identifier") === cercles.col("id"),"left_outer")
      .join(naceDetail, tiers.col("identifier") === naceDetail.col("id"),"left_outer")
      .drop("id")
      .filterDuplicates("duplicates_count", Seq("identifier"))

  }

}
