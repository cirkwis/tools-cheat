package com.socgen.bsc.cbs.businessviews.grc

import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer.{createStaticColumn, renameCol, _}
import com.socgen.bsc.cbs.businessviews.common.ExtraFunctionsDF._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Created by Minh-Hieu PHAM on 12/01/2017.
  */
class BuildGrcPpBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  val sqlContext = dataLoader.sqlContext
  override val frequency = Frequency.DAILY
  override val name = "grc_pp_bv"
  var schemaAvroPath = "avro"

  /**
    * Function: SelectFromSource
    * Description: Before realizing join task, we need to pr-select just used columns
    *
    * @return a map of [String, DataFrame] so each element is a pair of dataframe and its name (pp, pm, lien and cav).
    **/
  def selectFromSource(): Map[String, DataFrame] = {
    var _mapSource: Map[String, DataFrame] = Map()
    val grc_ppbigdata = eliminateDuplicates(dataLoader.load("grc/ppbigdata"), "PERSON_UID")
        .withColumnRenamed("X_POUVOIR_CAV","X_POUVOIR_CAV_PP")
        .withColumnRenamed("X_POUVOIR_AUT","X_POUVOIR_AUT_PP")
        .withColumnRenamed("X_POUVOIR_TIT","X_POUVOIR_TIT_PP")
        .cache()

    _mapSource = _mapSource + ("grc_ppbigdata" -> grc_ppbigdata)

    val pmSelect = List("NAME", "X_RAISON_SOCIALE")
    val grc_pmbigdata = eliminateDuplicates(dataLoader.load("grc/pmbigdata"), "NAME")
              .select(pmSelect.map(col): _*)
    _mapSource = _mapSource + ("grc_pmbigdata" -> grc_pmbigdata)

    val grc_ttbigdata = dataLoader.load("grc/ttbigdata")
    _mapSource = _mapSource + ("grc_ttbigdata" -> grc_ttbigdata)

    val lienTiersSelectPres = List("PARTY_UID", "OWNER_ASSET_NUM", "REL_TYPE_CD")
    val grc_libigdata = dataLoader.load("grc/libigdata").select(lienTiersSelectPres.map(col): _*).withColumnRenamed("REL_TYPE_CD","REL_TYPE_CD_PRES")
    _mapSource = _mapSource + ("grc_libigdata" -> grc_libigdata)

    val grc_prbigdata = dataLoader.load("grc/prbigdata")
    _mapSource = _mapSource + ("grc_prbigdata" -> grc_prbigdata)

    _mapSource
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

    val pm = lienDf
      .filter("TYPE_TIERS = 0 and TYPE_TIERS_LIE <> '00'")
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

  def groupingGrcPp(grc_ttbigdata: DataFrame): DataFrame = {
    val simpleGrcPpSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/pp.avsc")

    val grcPpType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("pp", ArrayType(simpleGrcPpSchema), true)))


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
      createStaticColumn("type_de_lien_economique","null"),
      createStaticColumn("nature_droits",null),
      createStaticColumn("statut_tiers_pp",null),
      createStaticColumn("type_tiers_pp",null)

    )

    val pp = grc_ttbigdata
      .filter("TYPE_TIERS = 0 and TYPE_TIERS_LIE = '00'")
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

  def groupingGrcPresta(grc_prbigdata: DataFrame, grc_lien_pres : DataFrame): DataFrame = {

    val simpleGrcCavSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcCav.avsc")
    val grcCavType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("presta", ArrayType(simpleGrcCavSchema), true)))

    val grc_pres = grc_lien_pres
      .select("OWNER_ASSET_NUM","REL_TYPE_CD_PRES", "PARTY_UID")
      .filter("OWNER_ASSET_NUM is not  null")
      .filter("PARTY_UID is not null")
      .withColumnRenamed("PARTY_UID", "id")
      .withColumnRenamed("OWNER_ASSET_NUM","id_pres")

    // Tranformation columns
    val prestations = grc_pres.join(grc_prbigdata, grc_pres.col("id_pres") === grc_prbigdata.col("OWNER_ASSET_NUM"),"left_outer").drop("id_pres")
      .applyOperations(transformationsCav)

    val rddRow =
      prestations.rdd
        .filter(_.getAs[String]("id") != null)
        .groupBy(r => r.getAs[String]("id"))
        .map {
          case (id: String, rows: Seq[Row]) => {
            val nrs = rows.map {
              case row => Row(simpleGrcCavSchema.fieldNames.map(row.getAs[String](_)): _*)
            }
            Row(id, nrs)
          }
        }
    sqlContext.createDataFrame(rddRow, grcCavType)
  }

  def groupingGrcCustomerRelationship(grc_pp: DataFrame): DataFrame = {
    val customerRelationshipSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/grcCustomerRelationship.avsc")

    val grcType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("customer_relationship_manager", customerRelationshipSchema, true)))

    val ppRdd = grc_pp.map( row =>  {
      Row(row.getAs("PERSON_UID"), (row.getAs("DESC_TXT_AGENCE"),row.getAs("EMAIL_ADDR"),row.getAs("WORK_PH_NUM"), row.getAs("LAST_NAME_CC")))
    })
    sqlContext.createDataFrame(ppRdd, grcType)
  }


  def prepareGrcPpBv(): DataFrame = {
    val _mapSource = selectFromSource()
    val grc_cav = _mapSource("grc_prbigdata").filter("OWNER_ASSET_NUM is not null")
    val grc_pp = _mapSource("grc_ppbigdata")
    val grc_lien_tiers = _mapSource("grc_libigdata")
    val grc_lien = _mapSource("grc_ttbigdata")
    val grc_pm = _mapSource("grc_pmbigdata")

    val resultSelect = List("activity", "bad_id", "bddf_crs_status", "birth_country_code", "birth_date", "insee_birth_place",
      "authority_account", "client_type", "compliance_3c_top",
      "contract_sg_end_date", "contract_sg_start_date", "country_code", "csm_code", "currency_country_code",
      "current_type_ownership", "customer_incident", "death_date", "dec_code", "dec_name", "dom_top", "dr_code",
      "dr_name", "eer_client_status", "email", "employee_presence_top", "employer_address", "employer_country_code",
      "employer_name", "employer_psa_code", "end_relationship_reason_code", "end_relationship_reason_label", "family_situation",
      "fax", "first_name", "foreign_birth_place", "gender_code", "global_monthly_revenus", "address", "address_city",
      "address_code_postal", "address_numero_voie", "identifier", "inactive_client_top", "incident_nature", "ind_risk_lab",
      "ind_risk_lab_update_date", "interdict_bdf_code", "internet_connect_number", "internet_mail_top", "job_title", "last_name",
      "last_update_crs_status_monaco", "last_update_bddf_crs_statut_date", "legal_capacity_code", "living_deceased_code",
      "maiden_name", "main_agency", "matrimonial_regime", "monaco_crs_status", "nationality", "nsm_code", "number_of_children",
      "number_of_dependents", "authority_other", "pcs_code", "pcs_code_niv2", "per_title", "phone_home", "phone_mobile",
      "phone_work", "politically_exposed", "privacy_indicator", "private_bank_top", "private_bank_top_update_date", "pro_status",
      "psa_code", "relationship_end_date", "relationship_start_date", "nature_of_client", "revenus", "risk_cotation_code",
      "risk_label", "risk_loss_relationship", "risk_loss_relationship_date", "authority_securities", "sg_personal_number",
      "sign_global_monthly_revenus", "sign_revenus", "tax_res_country_code", "tax_identifier", "top_expatriate",
      "transf_ceiling_bad")

    val prestations = groupingGrcPresta(grc_cav, grc_lien_tiers)
    val pp = groupingGrcPp(grc_lien)
    val crs = groupingGrcCustomerRelationship(grc_pp) // CustomerRelationship
    val pm = groupingGrcPm(grc_pm, grc_lien)
    val dfResult = grc_pp.applyOperations(transformations).select(resultSelect.map(col): _*)

    val result = dfResult.join(crs, dfResult.col("identifier") === crs.col("id"),"left_outer")
                         .join(prestations, dfResult.col("identifier") === prestations.col("id"),"left_outer")
                         .join(pp, dfResult.col("identifier") === pp.col("id"),"left_outer")
                         .join(pm, dfResult.col("identifier") === pm.col("id"),"left_outer")
                         .drop("id")
                         .filterDuplicates("duplicates_count", Seq("identifier"))

    result
  }





  /**
    * Value: transformations
    * Description: List all transformations to be realized (Rename and Concatenation)
    **/
  val transformations = List(

    renameCol("X_ACTIVITE", "activity"),
    renameCol("X_REF_EXTERNE", "bad_id"),
    renameCol("X_CRS_STATUT_BDDF", "bddf_crs_status"),
    renameCol("X_CODE_PAYS_NAIS", "birth_country_code"),
    renameCol("BIRTH_DT", "birth_date"),
    renameCol("X_INSEE_LIEU_NAIS", "insee_birth_place"),
    renameCol("X_POUVOIR_CAV_PP","authority_account"),
    renameCol("CON_CD", "client_type"),
    renameCol("X_3C_CONF_DOS","compliance_3c_top"),
    renameCol("X_FIN_EMP_DT", "contract_sg_end_date"),
    renameCol("X_DEB_EMP_DT", "contract_sg_start_date"),
    renameCol("COUNTRY", "country_code"),
    renameCol("X_CODE_CSM", "csm_code"),
    renameCol("X_CODE_PAYS_RMON", "currency_country_code"),
    renameCol("ATTRIB_34", "current_type_ownership"),
    renameCol("ATTRIB_11", "customer_incident"),
    renameCol("X_DCD_DT", "death_date"),
    renameCol("CODE_DEC", "dec_code"),
    renameCol("DESC_TXT_DEC", "dec_name"),
    renameCol("X_TOP_DOMICIL", "dom_top"),
    renameCol("CODE_DR", "dr_code"),
    renameCol("DESC_TXT_DR", "dr_name"),
    renameCol("X_EERAD_STATUT", "eer_client_status"),
    renameCol("EMAIL_ADDR_MAIL", "email"),
    renameCol("X_CODE_PRES_EMP", "employee_presence_top"),
    mergeFields("employer_address",concatenate(" "),"ADDR_EMP","ADDR_LINE_2_EMP","COUNTY_EMP","ADDR_LINE_3_EMP","X_PROVINCE_EMP","X_ZIPCODE_EMP","X_CITY_EMP"),


    renameCol("COUNTRY_EMP", "employer_country_code"),
    renameCol("X_NOM_EMPLOYEUR", "employer_name"),
    renameCol("X_PSA_EMP", "employer_psa_code"),
    renameCol("X_CODE_MODI_FERM", "end_relationship_reason_code"),
    renameCol("SG_MOTIF_FIN_RELATION", "end_relationship_reason_label"),
    renameCol("X_CODE_MARI_STAT", "family_situation"),
    renameCol("X_TEL_NOUV_NORM_FAX", "fax"),
    renameCol("X_PRENOM", "first_name"),
    renameCol("X_LIEU_NAISS_ETR", "foreign_birth_place"),
    renameCol("SEX_MF", "gender_code"),
    renameCol("ANNL_REVENUE", "global_monthly_revenus"),


    mergeFields("address",concatenate(" "),"ADDR","ADDR_LINE_2","COUNTY","ADDR_LINE_3","X_PROVINCE","X_ZIPCODE","X_CITY"),
    renameCol("X_CITY", "address_city"),
    renameCol("X_ZIPCODE", "address_code_postal"),
    renameCol("COUNTY", "address_numero_voie"),

    renameCol("PERSON_UID", "identifier"),
    renameCol("X_CLT_INACTIF", "inactive_client_top"),
    renameCol("X_INCI_QUALIF", "incident_nature"),
    renameCol("X_IND_RISQ_LAB", "ind_risk_lab"),
    renameCol("X_DT_IND_RL_UPD", "ind_risk_lab_update_date"),
    renameCol("ATTRIB_07", "interdict_bdf_code"),
    renameCol("X_SCORE_BAD", "internet_connect_number"),
    renameCol("X_WEB", "internet_mail_top"),
    renameCol("JOB_TITLE", "job_title"),
    renameCol("X_NOM", "last_name"),
    renameCol("X_DT_LST_CRS_FA_MONACO", "last_update_crs_status_monaco"),
    renameCol("X_DT_LST_CRS_FA_BDDF", "last_update_bddf_crs_statut_date"),
    renameCol("X_CODE_CAPA_JURI", "legal_capacity_code"),
    renameCol("X_ATTRIB_54", "living_deceased_code"),
    renameCol("MAIDEN_NAME", "maiden_name"),
    renameCol("AGENCE_PRINCIPALE_TIERS", "main_agency"),
    renameCol("X_CODE_REGI_MATR", "matrimonial_regime"),
    renameCol("X_CRS_STATUT_MONACO", "monaco_crs_status"),
    renameCol("X_CODE_NATION", "nationality"),
    renameCol("X_CODE_NSM", "nsm_code"),
    renameCol("X_NB_ENF", "number_of_children"),
    renameCol("X_NB_PERS_A_CHARGE", "number_of_dependents"),
    renameCol("X_POUVOIR_AUT_PP", "authority_other"),
    renameCol("X_CODE_PCS", "pcs_code"),
    renameCol("X_CODE_PCS_N3", "pcs_code_niv2"),
    renameCol("PER_TITLE", "per_title"),
    renameCol("X_TEL_NOUV_NORM_DOM", "phone_home"),
    renameCol("X_TEL_NOUV_NORM_MOB", "phone_mobile"),
    renameCol("X_TEL_NOUV_NORM_PRO", "phone_work"),
    renameCol("X_IND_PPE", "politically_exposed"),
    renameCol("X_CONFID", "privacy_indicator"),
    renameCol("X_BANQUE_PRIV", "private_bank_top"),
    renameCol("X_BANQUE_PRIV_DT", "private_bank_top_update_date"),
    renameCol("TMZONE_CD", "pro_status"),
    renameCol("X_PSA", "psa_code"),
    renameCol("X_ATTRIB_58", "relationship_end_date"),
    renameCol("X_ATTRIB_57", "relationship_start_date"),
    renameCol("X_CODE_NATU_TIER", "nature_of_client"),
    renameCol("X_MT_REVENU_DOM", "revenus"),
    renameCol("X_COTATION_RISK", "risk_cotation_code"),
    renameCol("S_LST_OF_VAL", "risk_label"),
    renameCol("X_SCORE_ATTRI", "risk_loss_relationship"),
    renameCol("X_DT_SCORE_ATTRI", "risk_loss_relationship_date"),
    renameCol("X_POUVOIR_TIT_PP", "authority_securities"),
    renameCol("EMP_NUM", "sg_personal_number"),
    renameCol("SIGNE_ANNL_REVENUE", "sign_global_monthly_revenus"),
    renameCol("X_SIGNE_MT_REVENU_DOM", "sign_revenus"),
    renameCol("X_CODE_PAYS_RFIS", "tax_res_country_code"),
    renameCol("X_IDENTIF_FISCAL", "tax_identifier"),
    renameCol("ATTRIB_04", "top_expatriate"),
    renameCol("PLAFOND_VIREMENT_BAD", "transf_ceiling_bad")

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


  override def process(): DataFrame = {
      prepareGrcPpBv()
 }
}
