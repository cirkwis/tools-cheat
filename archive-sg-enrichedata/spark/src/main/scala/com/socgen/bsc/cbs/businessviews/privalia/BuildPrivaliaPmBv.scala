package com.socgen.bsc.cbs.businessviews.privalia

import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer.{createStaticColumn, renameCol, _}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class BuildPrivaliaPmBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override def name = "privalia_pm_bv"

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

    val tiers = dataLoader.load("privalia_tiers").where("TYPERS = 'PM'")
    _mapSource = _mapSource + ("prv_tiers" -> tiers)

    val tiers_pm_pp = dataLoader.load("privalia_tiers_pm_pp")
    _mapSource = _mapSource + ("prv_tiers_pm_pp" -> tiers_pm_pp)

    _mapSource
  }


  /**
    * Construct array of cercles
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
      renameCol("NUPERS", "id"),
      renameCol("DATCER", "date_creation_cercle"),
      renameCol("NUMCER", "num_cercle"),
      renameCol("ROLCER", "role_tiers_cercle_code"),
      renameCol("LIBROLE", "role_tiers_cercle_name"),
      renameCol("TOPREF", "top_referent"),
      renameCol("TYPCER", "type_cercle")
    )

    val cercles = cercleDf.applyOperations(transformationsCercles)
    // Grouping data by identifier
    val rddRow =
      cercles.rdd
        .filter(_.getAs[String]("id") != null)
        .groupBy(r => r.getAs[String]("id"))
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
    * Construct Struct of NaceRecDetail
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


  def groupingPp(ppDf  : DataFrame): DataFrame = {
    val simpleppSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/pp.avsc")
    // StructType for Cav array
    val prvPpType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("pp", ArrayType(simpleppSchema), true)))

    val transformationsPp = List(
      renameCol("NUPE_PM", "id"),
      renameCol("NUMG_PP", "identifier"),
      renameCol("SOCCAP", "type_de_lien_economique"),
      renameCol("NADROI", "nature_droits"),
      renameCol("PARCAP", "pourc_detention"),
      renameCol("STATUT_PP", "statut_tiers_pp"),
      renameCol("TYPERS_PP", "type_tiers_pp"),
      createStaticColumn("code_nation", null),
      createStaticColumn("code_pays_nais",null),
      createStaticColumn("code_pays_rfis",null),
      createStaticColumn("code_pays_rmon",null),
      createStaticColumn("first_name",null),
      createStaticColumn("fonction_interv",null),
      createStaticColumn("last_name",null),
      createStaticColumn("authority_other",null),
      createStaticColumn("authority_account",null),
      createStaticColumn("authority_securities",null),
      createStaticColumn("ref_procuration",null),
      createStaticColumn("sous_type_indiv",null),
      createStaticColumn("type_de_lien",null)

    )

    val pp = ppDf.where("TYPERS_PP = 'PP'").applyOperations(transformationsPp)
    // Grouping data by identifier
    val rddRow =
      pp.rdd
        .filter(_.getAs[String]("id") != null)
        .groupBy(r => r.getAs[String]("id"))
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


  /**
    * Contruct final BV (PRV_PM_BV)
    * @return
    */
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
      renameCol("TOTFORT","patrimoine_level"),
      renameCol("TOPPPE","top_ppe"),
      renameCol("PPECOD","ppe_code"),
      renameCol("PPELIB","ppe_name"),
      renameCol("NUMPORT","privileged_phone"),
      renameCol("NUPERS","rct_identifier"),
      renameCol("DATENDREL","relationship_end_date"),
      renameCol("DATDEBREL","relationship_start_date"),
      renameCol("STATUT","relationship_type_status"),
      renameCol("RISKCOD","risk_code"),
      renameCol("RISKLIB","risk_label"),
      renameCol("ACTSECTCOD","secteur_activite_code"),
      renameCol("ACTSECTLIB","secteur_activite_name"),
      renameCol("TOPEER","top_conformity_eer"),
      renameCol("TOPCOMPRISK","top_risk_complete"),
      renameCol("TOPGIP","topgip"),
      renameCol("TOTAV","total_aum"),
      renameCol("TYPCON","type_contact_code"),
      renameCol("LIBTYPCNT","type_contact_name")
    )


    val _mapSource = selectFromSource()
    val prv_cercles = _mapSource("prv_cercles")
    val prv_tiers = _mapSource("prv_tiers")
    val prv_tiers_pm_pp = _mapSource("prv_tiers_pm_pp")

    val pp = groupingPp(prv_tiers_pm_pp)
    val cercles = groupingCercles(prv_cercles)
    val naceDetail = groupingNaceRevDetail(prv_tiers)

    val tiers = prv_tiers.applyOperations(transformations).where("client_type = 'PM'")

    tiers
      .join(pp, tiers.col("rct_identifier") === pp.col("id"),"left_outer")
      .join(cercles, tiers.col("rct_identifier") === cercles.col("id"),"left_outer")
      .join(naceDetail, tiers.col("rct_identifier") === naceDetail.col("id"),"left_outer")
      .drop("id")
      .filterDuplicates("duplicates_count", Seq("rct_identifier"))

  }

}
