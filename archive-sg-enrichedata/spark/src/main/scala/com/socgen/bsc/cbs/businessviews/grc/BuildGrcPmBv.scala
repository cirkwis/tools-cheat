package com.socgen.bsc.cbs.businessviews.grc

import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer.{createStaticColumn, renameCol, _}
import com.socgen.bsc.cbs.businessviews.common.ExtraFunctionsDF._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by X156170 on 22/11/2016.
  */
class BuildGrcPmBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "grc_pm_bv"
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

    val ppSelect = List("PERSON_UID", "X_CODE_NATION", "X_CODE_PAYS_NAIS", "X_CITY")
    val pp = eliminateDuplicates(dataLoader.load("grc/ppbigdata"), "PERSON_UID")
              .select(ppSelect.map(col): _*)
    _mapSource = _mapSource + ("grc_ppbigdata" -> pp)

    val grc_pmbigdata =  eliminateDuplicates(dataLoader.load("grc/pmbigdata"), "NAME")
      .applyOperations(List(applySpecificOperation("X_NUM_SIRET", BuildGrcPmBv.clean_siren_identifier(), "X_NUM_SIRET")), true)
      .cache()

    _mapSource = _mapSource + ("grc_pmbigdata" -> grc_pmbigdata)


    val grc_ttbigdata = dataLoader.load("grc/ttbigdata")
    _mapSource = _mapSource + ("grc_ttbigdata" -> grc_ttbigdata)


    val grc_prbigdata = dataLoader.load("grc/prbigdata")
    _mapSource = _mapSource + ("grc_prbigdata" -> grc_prbigdata)


    val grc_libigdata = dataLoader.load("grc/libigdata").withColumnRenamed("REL_TYPE_CD","REL_TYPE_CD_PRES")
    _mapSource = _mapSource + ("grc_libigdata" -> grc_libigdata)


    _mapSource
  }

  val transformationsPp = List(
    renameCol("UID_TIERS", "id"),
    renameCol("UID_TIERS_LIE", "identifier"),
    renameCol("FST_NAME_PP", "first_name"),
    renameCol("LAST_NAME_PP","last_name"),
    renameCol("REL_TYPE_CD","type_de_lien"),
    renameCol("X_REF_PROC", "ref_procuration"), // liens
    renameCol("X_FONCTION_INTERV", "fonction_interv"), //liens
    renameCol("X_POUVOIR_CAV", "authority_account"),
    renameCol("X_POUVOIR_TIT", "authority_securities"),
    renameCol("X_POUVOIR_AUT", "authority_other"),
    renameCol("X_POUR_DETENU", "pourc_detention"),
    renameCol("X_SS_TYPE_INDIV","sous_type_indiv"),
    renameCol("X_CODE_NATION_PP", "code_nation"),
    renameCol("X_CODE_PAYS_RFIS_PP", "code_pays_rfis"),
    renameCol("X_CODE_PAYS_NAIS_PP", "code_pays_nais"),
    renameCol("X_CODE_PAYS_RMON_PP", "code_pays_rmon"),
    createStaticColumn("type_de_lien_economique",null),
    createStaticColumn("nature_droits",null),
    createStaticColumn("statut_tiers_pp",null),
    createStaticColumn("type_tiers_pp",null)

  )


  val transformationsCav = List(
    identityCol("id"),
    renameCol("X_NUM_ADRESSE", "address_type"),
    renameCol("ENCOURS", "amount_of_outstanding"),
    renameCol("APY", "bank_code"),
    renameCol("REL_LIMIT", "bank_office_code"),
    renameCol("X_SS_PROD", "byproduct_code"),
    renameCol("X_NUM_SIRET", "cg_siret"),
    renameCol("X_NU_CLIENT", "client_num"),
    renameCol("X_CLE_NU_CLIENT", "client_num_key"),
    renameCol("X_TYPE_CLIENT", "client_type"),
    renameCol("END_DT", "closing_date"),
    renameCol("DATE_ECHEANCE", "due_date"),
    renameCol("X_TOP_TITULAIRE", "holder"),
    renameCol("IDCPLI1", "id_of_account"),
    renameCol("OWNER_ASSET_NUM", "identifier"),
    renameCol("X_SG_CP_CDENJU", "legal_entity"),
    renameCol("LAST_NAME", "main_advisor"),
    renameCol("PARTY_UID", "main_third_id"),
    renameCol("MTAUPA", "mtaupa"),
    renameCol("MTAURE", "mtaure"),
    renameCol("MTNOVB3", "mtnovb3"),
    renameCol("MTNOVB9", "mtnovb9"),
    renameCol("START_DT", "opening_date"),
    renameCol("IDCLO1", "pm_bad_id"),
    renameCol("CURCY_CD", "currency_code"),
    renameCol("METER_CNT", "product_code"),
    renameCol("CDMFGP", "reason_close_insurance"),
    renameCol("X_SG_CP_CDUNIC", "rib_key"),
    renameCol("SIGNE_ENCOURS", "signe_amount_of_outstanding"),
    renameCol("SIGNE_MTAUPA", "signe_mtaupa"),
    renameCol("SIGNE_MATURE", "signe_mtaure"),
    renameCol("SIGNE_MTNOVB3", "signe_mtnovb3"),
    renameCol("SIGNE_MTNOVB9", "signe_mtnovb9"),
    renameCol("X_SG_CODESRC", "source_code"),
    renameCol("STATUS_CD", "status"),
    renameCol("CDOPFT", "top_sbb"),
    renameCol("X_TOP_VP_VD", "top_vp_vd"),
    renameCol("REL_TYPE_CD_PRES", "role_tiers_presta")
    )




  /**
      * Value: transformations
      * Description: List all transformations to be realized (Rename and Concatenation)
      **/
    val transformations = List(

      renameCol("X_CITY", "city"),
      renameCol("OU_TYPE_CD", "client_type"),
      renameCol("FISC_YEAR_END", "company_creation_date"),
      renameCol("X_3C_CONF_DOS", "compliance_3c_top"),
      renameCol("X_CODE_PAYS_FISC", "country_tax_code"),
      renameCol("X_CODE_CSM", "csm_code"),
      renameCol("X_CODE_PAYS_MONE", "currency_country_code"),
      renameCol("CODE_DEC", "dec_code"),
      renameCol("DESC_TXT_DEC", "dec_name"),
      renameCol("X_TOP_DOMICIL", "dom_top"),
      renameCol("CODE_DR", "dr_code"),
      renameCol("DESC_TXT_DR", "dr_name"),
      renameCol("EMP_COUNT", "employees_total"),
      renameCol("ATTRIB_08", "end_relationship_reason_code"),
      mergeFields("headquarters_address", concatenate(" "), "ADDR", "ADDR_LINE_2", "ADDR_LINE_3", "COUNTY", "X_PROVINCE", "X_ZIPCODE", "city"),
      renameCol("X_ZIPCODE", "headquarters_address_code_postal"),
      renameCol("COUNTRY", "headquarters_address_country_code"),
      renameCol("ADDR_LINE_2", "headquarters_address_diverse"),
      renameCol("ADDR_LINE_3", "headquarters_address_diverse_suppl"),
      renameCol("X_PROVINCE", "headquarters_address_locality"),
      renameCol("COUNTY", "headquarters_address_numero_voie"),
      renameCol("ADDR", "headquarters_address_title"),

      renameCol("NAME", "identifier"),
      renameCol("X_CLT_INACTIF", "inactive_client_top"),
      renameCol("X_INCI_QUALIF", "incident_nature"),

      renameCol("X_IND_RISQ_LAB", "ind_risk_lab"),

      renameCol("X_DT_IND_Rl_UPD", "ind_risk_lab_update_date"),
      renameCol("X_SS_TYPE_INDIVISION", "indiv_subtype"),

      renameCol("X_INTERDIT_BANC", "interdict_bdf_code"),
      renameCol("X_CODE_CAT_JURI", "legal_category_code"),
      renameCol("X_DT_LST_CRS_FA_BDDF", "last_update_bddf_crs_status"),
      renameCol("X_DT_LST_CRS_FA_MONACO", "last_update_crs_status_monaco"),
      renameCol("X_CODE_FORM_JURI", "legal_form"),
      renameCol("X_RAISON_SOCIALE", "legal_name_90c"),
      renameCol("AGENCE_PRINCIPALE_TIERS", "main_agency"),
      renameCol("X_CRS_STATUT_MONACO", "monaco_crs_status"),
      renameCol("X_CODE_NAF", "naf_code"),
      renameCol("X_CODE_NATION_PM", "nationality_country_code"),
      renameCol("X_CODE_NSM", "nsm_code"),
      renameCol("X_CODE_SEC_SUIVI", "pcru_code"),

      renameCol("X_BANQUE_PRIV", "private_bank_top"),


      renameCol("X_BANQUE_PRIV_DT", "private_bank_top_update_date"),
      renameCol("X_TEL_NOUV_NORM", "privileged_phone"),

      renameCol("X_PSA", "psa_code"),
      renameCol("OU_NUM", "rct_identifier"),
      renameCol("CLOSE_DT", "relationship_end_date"),
      renameCol("REFERENCE_START_DT", "relationship_start_date"),

      renameCol("X_TOP_INCIDENT", "client_incident"),
      renameCol("TYPE", "relationship_type_status"),
      renameCol("X_COTATION_RISK", "risk_cotation_code"),
      renameCol("S_LST_OF_VAL", "risk_label"),
      renameCol("X_SCORE_ATTRI", "risk_loss_relationship"),
      renameCol("X_DT_SCORE_ATTRI", "risk_loss_relationship_date"),
      renameCol("X_TOP_SBB", "sbb_top"),
      renameCol("SIGNE_ATTRIB_15", "sign_ca_export_year"),
      renameCol("SIGNE_ATTRIB_24", "sign_ca_ht"),
      renameCol("ATTRIB_15", "ca_export_year"),
      renameCol("ATTRIB_24", "ca_ht"),
      renameCol("X_PAYS_LOC_ACT", "assets_country_localisation"),
      renameCol("X_CRS_STATUT_BDDF", "bddf_crs_status"),
      renameCol("X_NUM_SIRET", "siren_identifier"),
      renameCol("X_CODE_SSR", "ssr_code"),
      renameCol("X_IDENTIF_FISCAL", "tax_id"),
      renameCol("ALIAS_NAME", "trade_as_name")
    )


  def prepareGrcPmBv() : DataFrame = {

    val pmSelect = List(
      "headquarters_address", "siren_identifier","client_type","relationship_type_status","compliance_3c_top","private_bank_top",
      "incident_nature","ind_risk_lab","psa_code","indiv_subtype","client_incident", "headquarters_address_title","headquarters_address_diverse",
      "headquarters_address_diverse_suppl","main_agency","trade_as_name","end_relationship_reason_code","ca_export_year","ca_ht",
      "relationship_end_date","dec_code","dr_code","headquarters_address_country_code",
      "headquarters_address_numero_voie","dec_name","dr_name","employees_total","company_creation_date","identifier","rct_identifier",
      "relationship_start_date","risk_label","sign_ca_export_year","sign_ca_ht","private_bank_top_update_date", "city","inactive_client_top",
      "legal_category_code","csm_code","legal_form","naf_code","nationality_country_code","nsm_code","country_tax_code","currency_country_code","pcru_code","ssr_code",
      "risk_cotation_code","bddf_crs_status","monaco_crs_status","ind_risk_lab_update_date","last_update_bddf_crs_status","last_update_crs_status_monaco",
      "risk_loss_relationship_date","tax_id","interdict_bdf_code","assets_country_localisation",
      "headquarters_address_locality","legal_name_90c","risk_loss_relationship",
      "privileged_phone","dom_top","sbb_top","headquarters_address_code_postal")

    val _mapSource = selectFromSource()
    val grc_ttbigdata = _mapSource("grc_ttbigdata")
    val grc_prbigdata = _mapSource("grc_prbigdata").filter("OWNER_ASSET_NUM is not null")
    val grc_pmbigdata = _mapSource("grc_pmbigdata")
    val grc_libigdata = _mapSource("grc_libigdata")


    val presta = groupingGrcPresta(grc_prbigdata, grc_libigdata)
    val pp = groupingGrcPp(grc_ttbigdata)
    val pm = groupingGrcPm(grc_pmbigdata, grc_ttbigdata)
    val addHolder = groupingGrcAddHolder(grc_pmbigdata)
    val holder = groupingGrcHolder(grc_pmbigdata)
    val crs = groupingGrcCustomerRelationship(grc_pmbigdata) // CustomerRelationship
    val ceo = groupingGrcChiefExecutiveOfficer(grc_pmbigdata) // ChiefExecutiveOfficer
    val dfResult = grc_pmbigdata.applyOperations(transformations).select(pmSelect.map(col): _*)

    dfResult.join(presta, dfResult.col("identifier") === presta.col("id"),"left_outer")
          .join(pp, dfResult.col("identifier") === pp.col("id"),"left_outer")
          .join(pm, dfResult.col("identifier") === pm.col("id"),"left_outer")
          .join(addHolder, dfResult.col("identifier") === addHolder.col("id"),"left_outer")
          .join(holder, dfResult.col("identifier") === holder.col("id"),"left_outer")
          .join(crs, dfResult.col("identifier") === crs.col("id"),"left_outer")
          .join(ceo, dfResult.col("identifier") === ceo.col("id"),"left_outer")
          .drop("id")
          .filterDuplicates("duplicates_count", Seq("identifier"))

  }

  def groupingGrcPm(pmDf: DataFrame, lienDf : DataFrame): DataFrame = {
    val simpleGrcPmSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/pm.avsc")
    val transformationsPm = List(
      renameCol("UID_TIERS", "id"),
      renameCol("NAME", "identifier"),
      renameCol("X_RAISON_SOCIALE", "legal_name_90c"),
      renameCol("REL_TYPE_CD","type_de_lien"),
      createStaticColumn("type_de_lien_economique", null),
      createStaticColumn("nature_droits", null),
      createStaticColumn("pourc_detention", null),
      createStaticColumn("rct_identifier", null),
      createStaticColumn("statut_tiers_pm", null),
      createStaticColumn("type_tiers_pm", null)


    )

    //pmDf.show
    //lienDf.show

    val pm = lienDf
      .filter("TYPE_TIERS <> 0 and TYPE_TIERS_LIE <> '00'")
      .join(pmDf, lienDf.col("UID_TIERS_LIE") === pmDf.col("NAME"))
      .applyOperations(transformationsPm)

    val grcPmType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("pm", ArrayType(simpleGrcPmSchema), true)))

    val rddRow =
      pm.rdd
      .filter(_.getAs[String]("id") != null)
      .groupBy(r => r.getAs[String]("id"))
      .map {
        case (id: String, rows: Seq[Row]) => {
          val nrs = rows.map {
            case row => Row(simpleGrcPmSchema.fieldNames.map(row.getAs[String](_)): _*)
          }
          Row(id, nrs)
        }
      }

    sqlContext.createDataFrame(rddRow, grcPmType)
  }

  def groupingGrcPresta(grc_prbigdata: DataFrame, grc_libigdata : DataFrame): DataFrame = {

    val simplePrestationSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcCav.avsc")
    val grc_pres = grc_libigdata.select("OWNER_ASSET_NUM","REL_TYPE_CD_PRES", "PARTY_UID")
        .filter("OWNER_ASSET_NUM is not null")
        .filter("PARTY_UID is not null")
        .withColumnRenamed("PARTY_UID", "id")
        .withColumnRenamed("OWNER_ASSET_NUM","id_pres")

    // Tranformation columns
    val prestations = grc_pres.join(grc_prbigdata, grc_pres.col("id_pres") === grc_prbigdata.col("OWNER_ASSET_NUM"),"left_outer").drop("id_pres")
      .applyOperations(transformationsCav)

    // StructType for Cav array
    val grcPrestationType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("presta", ArrayType(simplePrestationSchema), true)))


    // Grouping data by identifier
    val rddRow =
      prestations.rdd
        .filter(_.getAs[String]("id") != null)
        .groupBy(r => r.getAs[String]("id"))
        .map {
          case (id: String, rows: Seq[Row]) => {
            val nrs = rows.map {
              case row => Row(simplePrestationSchema.fieldNames.map(row.getAs[String](_)): _*)
            }
            Row(id, nrs)
          }
        }
    sqlContext.createDataFrame(rddRow, grcPrestationType)
  }

  def groupingGrcPp(grc_ttbigdata: DataFrame): DataFrame = {
    val simpleGrcPpSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/pp.avsc")

    val grcPpType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("pp", ArrayType(simpleGrcPpSchema), true)))

    val pp = grc_ttbigdata
      .filter("TYPE_TIERS <> '00' and TYPE_TIERS_LIE = '00'")
      .applyOperations(transformationsPp)

    val rddRow =
      pp.rdd
      .filter(_.getAs[String]("id") != null)
      .groupBy(r => r.getAs[String]("id"))
      .map {
        case (id: String, rows: Seq[Row]) => {
          val nrs = rows.map {
            case row => Row(simpleGrcPpSchema.fieldNames.map(row.getAs[String](_)): _*)
          }
          Row(id, nrs)
        }
      }
    sqlContext.createDataFrame(rddRow, grcPpType)
  }

  def groupingGrcAddHolder(grc_pm: DataFrame): DataFrame = {
    val simpleGrcAddHolderSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcAddHolder.avsc")

    val grcAddHolderType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("add_holder", ArrayType(simpleGrcAddHolderSchema), true)))
    val pmRdd = grc_pm.map( row =>  {
      val act1 = (row.getAs("X_NOM_ACTI_1"),row.getAs("X_PCT_ACTI_1"))
      val act2 = (row.getAs("X_NOM_ACTI_2"),row.getAs("X_PCT_ACTI_2"))
      val act3 = (row.getAs("X_NOM_ACTI_3"),row.getAs("X_PCT_ACTI_3"))
      Row(row.getAs("NAME"), Array(act1,act2,act3))
    })
    sqlContext.createDataFrame(pmRdd, grcAddHolderType)
  }

  def groupingGrcHolder(grc_pm: DataFrame): DataFrame = {
    val simpleGrcAddHolderSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcHolder.avsc")

    val grcAddHolderType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("holder", ArrayType(simpleGrcAddHolderSchema), true)))
    val pmRdd = grc_pm.map( row =>  {
      val act1 = (row.getAs("X_NOM_ACTI1"),row.getAs("ATTRIB_04"),row.getAs("X_CODE_TYPE_ACTI1"))
      val act2 = (row.getAs("X_NOM_ACTI2"),row.getAs("ATTRIB_05"),row.getAs("X_CODE_TYPE_ACTI2"))
      Row(row.getAs("NAME"), Array(act1,act2))
    })
    sqlContext.createDataFrame(pmRdd, grcAddHolderType)
  }

  def groupingGrcCustomerRelationship(grc_pm: DataFrame): DataFrame = {
    val customerRelationshipSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcCustomerRelationship.avsc")

    val grcType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("customer_relationship_manager", customerRelationshipSchema, true)))

    val pmRdd = grc_pm.map( row =>  {
      Row(row.getAs("NAME"), (row.getAs("DESC_TXT_AGENCE"),row.getAs("EMAIL_ADDR"),row.getAs("WORK_PH_NUM"), row.getAs("LAST_NAME")))
    })
    sqlContext.createDataFrame(pmRdd, grcType)
  }

  def groupingGrcChiefExecutiveOfficer(grc_pm: DataFrame): DataFrame = {
    val customerRelationshipSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcChiefExecutiveOfficer.avsc")

    val grcType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("chief_executive_officer", customerRelationshipSchema, true)))

    val pmRdd = grc_pm.map( row =>  {
      Row(row.getAs("NAME"), (row.getAs("ATTRIB_01"),row.getAs("ATTRIB_02")))
    })
    sqlContext.createDataFrame(pmRdd, grcType)
  }



  override def process(): DataFrame = {
    prepareGrcPmBv()
  }
}

object BuildGrcPmBv {
  def clean_siren_identifier() = (vars: Seq[String]) => {
    val x_num_siret = vars(0)
    var result = ""
    x_num_siret match {
      case null => result = null
      case _ => result = x_num_siret.takeRight(9)
    }
    result
  }
}