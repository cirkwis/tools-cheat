package com.socgen.bsc.cbs.businessviews.dun

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._

/**
  * Created by X150102 on 21/11/2016.
  */
class BuildDunLeBv (override val dataLoader: DataFrameLoader) extends BusinessView {

  override val frequency = Frequency.DAILY
  override val name = "dun_le_bv"

  def getNaer5Code() = (vars: Seq[String]) => {
    val codeLocalPrimair = vars(0)
    val indicActivity = vars(1)
    var result = ""

    if("015".equals(indicActivity)){
      result = codeLocalPrimair
    }
    result
  }

  def getOtherNationalId() = (vars: Seq[String]) => {
    val identifiantNation = vars(0)
    val systIdentifNat = vars(1)
    var result = ""
    try{
      val systIdentifNatInteger = systIdentifNat.toInt
      if( systIdentifNatInteger != 17){
        result = identifiantNation
      }
    }catch {
      case e :Exception => None
    }
    result
  }

  def getSiren() = (vars: Seq[String]) => {
    val identifiantNation = vars(0)
    val systIdentifNat = vars(1)
    var result = ""
    try{
      val systIdentifNatInteger = systIdentifNat.toInt
      if( systIdentifNatInteger == 17){
        result = identifiantNation.substring(0,9)
      }
    }catch {
      case e :Exception => None
    }
    result
  }

  def getSiret() = (vars: Seq[String]) => {
    val identifiantNation = vars(0)
    val systIdentifNat = vars(1)
    var result = ""
    try{
      val systIdentifNatInteger = systIdentifNat.toInt
      if( systIdentifNatInteger == 17){
        result = identifiantNation
      }
    }catch {
      case e :Exception => None
    }
    result
  }

  val dunFinaltransformations = List (
    renameCol("SIGLE","acronym"),
    mergeFields("address_du",concatenate(" "),"ADRESSE_GR_PAYS","CODE_POSTAL_GR_PAY", "VILLE_GR_PAYS" ),
    mergeFields("address_gu",concatenate(" "),"ADRESSE_GR_MONDIAL","CODE_POSTAL_GR_MON", "VILLE_GR_MONDIAL" ),
    mergeFields("address_hq",concatenate(" "),"ADRESSE_MM","CODE_POSTAL_MM", "VILLE_MM" ),
    renameCol("NOM_RAIS_SOC_GR_PA","business_name_du"),
    renameCol("NOM_RAIS_SOC_GR_MO","business_name_gu"),
    renameCol("NOM_RAIS_SOC_MM","business_name_hq"),
    renameCol("NOM_DIRIGEANT","chief_executive_officer_name"),
    renameCol("FONCTION_DIRIGEANT","chief_executive_officer_title"),
    renameCol("VILLE","city"),
    renameCol("CODE_CONTINENT","continent"),
    renameCol("DATE_CREATION","creation_date"),
    renameCol("CODE_DEVISE","currency_code"),
    renameCol("CODE_PAYS","dest_country"),
    renameCol("CODE_PAYS_GR_PAYS","dest_country_du"),
    renameCol("CODE_PAYS_GR_MONDI","dest_country_gu"),
    renameCol("CODE_PAYS_MM","dest_country_hq"),
    renameCol("DUN_NUMBER","dun_id"),
    renameCol("DUN_NUMBER_GR_PAYS","duns_number_du"),
    renameCol("DUN_NUMBER_GR_MOND","duns_number_gu"),
    renameCol("DUN_NUMBER_MM","duns_number_hq"),
    renameCol("EFFECTIF_SITE","employees_here"),
    renameCol("NOMBRE_MEMBRE_FAMI","family_members_number_gu"),
    renameCol("DATE_MAJ_FAMILLE","family_update_date"),
    renameCol("TMP_DUN_NUMBER_MM_COUNT","group_number_legal_entities"),
    renameCol("FORME_JURIDIQUE","legal_form"),
    renameCol("NOM_RAIS_SOC","legal_name_90c"),
    renameCol("INDIC_STATUT","legal_situation_code"),
    renameCol("CA_ANNUEL_LOCAL","local_annual_sales"),
    mergeFields("mailing_address",concatenate(" "),"ADRESSE_MAIL","CODE_POSTAL_MAIL", "VILLE_MAIL", "NOM_MAIL_ETAT_PROV" ),
    applySpecificOperation("naer5_code", getNaer5Code(), "CODE_LOCAL_PRIMAIR", "INDIC_ACTIVITE"),
    applySpecificOperation("other_national_id", getOtherNationalId(), "IDENTIFIANT_NATION", "SYST_IDENTIF_NAT"),
    mergeFields("physical_address",concatenate(" "),"ADRESSE_1","ADRESSE_2", "CODE_POSTAL", "city", "ETAT_PROVINCE"),
    renameCol("IND_SOURCE","registered_address_indicator"),
    renameCol("DATE_RAPPORT","report_date"),
    renameCol("CODE_SIC2","sic_cod_1987_2"),
    renameCol("CODE_SIC3","sic_cod_1987_3"),
    renameCol("CODE_SIC4","sic_cod_1987_4"),
    renameCol("CODE_SIC5","sic_cod_1987_5"),
    renameCol("CODE_SIC6","sic_cod_1987_6"),
    renameCol("CODE_SIC1","sic_code_1987"),
    applySpecificOperation("siren", getSiren(), "IDENTIFIANT_NATION", "SYST_IDENTIF_NAT"),
    applySpecificOperation("siret", getSiret(), "IDENTIFIANT_NATION", "SYST_IDENTIF_NAT"),
    renameCol("CODE_STATUT","status"),
    renameCol("CODE_FILIALE","subsidiary"),
    renameCol("EFFECTIF_SITE_TOT","total_employees"),
    renameCol("CA_ANNUEL_DOLLAR_U","us_dol_annual_sales")
  )

  override def process(): DataFrame = {
    val dunInitialSource = dataLoader.load("dun")

    /*Calculate DUN_NUMBER_MM in current set of data*/
    val countDunNumberMm = dunInitialSource.select("DUN_NUMBER_MM")
      .groupBy("DUN_NUMBER_MM")
      .count()
      .applyOperations(List( renameCol("DUN_NUMBER_MM","TMP_DUN_NUMBER_MM"), renameCol("count","TMP_DUN_NUMBER_MM_COUNT"), castColumn("TMP_DUN_NUMBER_MM_COUNT","String")), true)

    /*Join DUN_NUMBER_MM caculation to inital data*/
    val resJointure = dunInitialSource.join(countDunNumberMm,col("TMP_DUN_NUMBER_MM") === col("DUN_NUMBER_MM"), "left_outer")

    /*Apply final transformation, & eliminating doublons*/
    resJointure.applyOperations(dunFinaltransformations).filter("dun_id is not null").filterDuplicates("duplicates_count", Seq("dun_id"))
  }
}
