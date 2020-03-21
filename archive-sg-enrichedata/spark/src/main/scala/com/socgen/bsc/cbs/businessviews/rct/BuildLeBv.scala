package com.socgen.bsc.cbs.businessviews.rct

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.rct.BuildLeBv._

class BuildLeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "rct_le_bv"

  val sqlContext = dataLoader.sqlContext

  import sqlContext.implicits._

  override def process(): DataFrame = {
    var result: DataFrame = null

    // Join Cdnpemot & Cdnsuivt
    val cdnpemot = prepareCdnpemot()
    val cdnsuivt = prepareCdnsuivt()
    result = cdnpemot.join(cdnsuivt, $"NUMERO" === cdnsuivt("numero_pm"), "left_outer")

    // Join Result & Spmelrpt
    val spmelrpt = prepareSpmelrpt()
    result = result.join(spmelrpt, $"NUMERO" === spmelrpt("NUPERS_MAP"), "left_outer")

    // Join Result & Spmsrpmt
    val isParentCompanyUdf = udf(isParentCompany)
    val scoringIsParentCompanyUdf = udf(scoringIsParentCompany)

    val (spmsrpmt_gs, spmsrpmt_sg) = prepareSpmsrpmt()
    result = result.join(spmsrpmt_gs, $"NUMERO" === spmsrpmt_gs("NUMERO_PM_GS"), "left_outer")
      .withColumn("scoring_is_parent_company", scoringIsParentCompanyUdf(col("CODE_ROLE_PM")))
      .withColumn("is_parent_company", isParentCompanyUdf(col("CODE_ROLE_PM")))

    result = result.join(spmsrpmt_sg, $"NUMERO" === spmsrpmt_sg("NUMERO_PM_SG"), "left_outer")

    // Prepare the last view
    result.select($"NUMERO_PM_ABSORBANTE".as("acquiring_le_identifier"),
      $"SIGLE".as("acronym"),
      $"activity_follow_up_code",
      $"CODE_APE_INSEE5".as("ape_insee5_code"),
      $"CODE_APE5".as("ape5_code"),
      $"big_regulatory_risk_code",
      $"DATE_EXPIRATION_ENTREPRIS".as("business_expiry_date"),
      $"TYPE_CLIENT".as("client_type"),
      $"DATE_CREATION_ENTREPRISE".as("company_creation_date"),
      $"commercial_follow_up_code",
      $"COMPLEMENT_DEST".as("complement_dest_address"),
      $"delegated_pcru",
      $"CODE_PAYS_DEST".as("dest_country_code"),
      $"CODELR_MAP".as("elr_code"),
      $"RASOELR_MAP".as("elr_legal_name"),
      $"elr_status",
      $"CODE_MOTIF_FIN_RELATION".as("end_relationship_reason_code"),
      $"FLAGPRUD".as("flagprud"),
      $"headquarters_address",
      $"IDENTIFICATION_DEST".as("identification_dest_address"),
      $"NUMERO".as("identifier"),
      $"CODE_CATEGORIE_JURIDIQUE".as("legal_category_code"),
      $"RAISON_SOCIALE".as("legal_name"),
      $"RAISON_SOCIALE_90C".as("legal_name_90c"),
      $"list_follow_up_code",
      $"CODE_SITUATION_JURIDIQUE".as("legal_situation_code"),
      $"CODE_NAER5".as("naer5_code"),
      $"operational_center",
      $"CODE_NESSG".as("nessg_code"),
      $"DATE_CESSATION_ACTIV_ENTR".as("out_business_date"),
      $"pcru_code",
      $"DATE_CESSATION_PAIEMENT".as("payments_suspension_date"),
      $"principal_operating_entity_code",
      $"profitability_study",
      $"registration_entity",
      $"BUREAU_DISTRIBUTEUR_ADRES".as("post_office_address"),
      $"DATE_FIN_RELATION".as("relationship_end_date"),
      $"DATE_ENTREE_RELATION".as("relationship_start_date"),
      $"relationship_status",
      $"STATUT_TYPE_RELATION".as("relationship_type_status"),
      $"risk_unit",
      $"NUMERO_SIREN".as("siren_identifier"),
      $"CODE_CAT_TIERS_REGLEM".as("statutory_third_parties_cat_code"),
      $"technical_follow_up",
      $"TOP_EMBARGO".as("top_embargo"),
      $"transverse_follow_up",
      $"le_group_id",
      $"le_subgroup_id",
      $"scoring_is_parent_company",
      $"is_parent_company",
      $"headquarters_address_complement_geo",
      $"headquarters_address_lieu_dit",
      $"headquarters_address_numero_voie",
      $"headquarters_address_code_postal"
    )
  }

  def prepareCdnpemot(): DataFrame = {
    val transformations = List(
      applySpecificOperation("relationship_status", dateFinRelationFunction(), "DATE_FIN_RELATION"),
      mergeFields("headquarters_address", concatenate(" "), "COMPLEMENT_GEO", "NUMERO_VOIE", "LIEU_DIT", "CODE_POSTAL_LOCALITE"),
      applySpecificOperation("delegated_pcru", delegatedPcruFunction(), "CODE_NATURE_SUIVI1", "CODE_NATURE_SUIVI2", "CODE_NATURE_SUIVI3", "CODE_NATURE_SUIVI4", "CODE_NATURE_SUIVI5",
        "INDIC_ORG_SUIVI_RISQUE1", "INDIC_ORG_SUIVI_RISQUE2", "INDIC_ORG_SUIVI_RISQUE3", "INDIC_ORG_SUIVI_RISQUE4", "INDIC_ORG_SUIVI_RISQUE5"),
      renameCol("COMPLEMENT_GEO", "headquarters_address_complement_geo"),
      renameCol("NUMERO_VOIE", "headquarters_address_numero_voie"),
      renameCol("LIEU_DIT", "headquarters_address_lieu_dit"),
      renameCol("CODE_POSTAL_LOCALITE", "headquarters_address_code_postal")
    )

    val df = dataLoader.load("rct_cdnpemot")
    df.applyOperations(transformations, true)
  }

  def prepareCdnsuivt(): DataFrame = {
    // Calculate fields [activity_follow_up_code, big_regulatory_risk_code, commercial_follow_up_code, list_follow_up_code, operational_center, pcru_code, principal_operating_entity_code, profitability_study, registration_entity, risk_unit, technical_follow_up, transverse_follow_up]
    dataLoader.load("rct_cdnsuivt")
      .select("NUMERO_PM", "CODE_NATURE_SUIVI", "IDENTIFIANT_ES")
      .filter("NUMERO_PM is not null")
      .rdd.groupBy(r => r.get(0))
      .map { case (n: String, l: Seq[Row]) =>

        var m: Map[String, String] = Map()
        l.map { row => {
          val cns = row.getAs[String](1)
          var id: String = null
          if (!row.isNullAt(2))
            id = row.getAs[String](2)
          m += (cns -> id)
        }
        }

        val activity_follow_up_code = m.get("03").getOrElse(null)
        val big_regulatory_risk_code = m.get("11").getOrElse(null)
        val commercial_follow_up_code = m.get("06").getOrElse(null)
        val list_follow_up_code = m.get("12").getOrElse(null)
        val operational_center = m.get("09").getOrElse(null)
        val pcru_code = m.get("01").getOrElse(null)
        val principal_operating_entity_code = m.get("05").getOrElse(null)
        val profitability_study = m.get("02").getOrElse(null)
        val registration_entity = m.get("10").getOrElse(null)
        val risk_unit = m.get("08").getOrElse(null)
        val technical_follow_up = m.get("07").getOrElse(null)
        val transverse_follow_up = m.get("04").getOrElse(null)

        Cdnsuivt(n, activity_follow_up_code, big_regulatory_risk_code, commercial_follow_up_code, list_follow_up_code, operational_center, pcru_code,
          principal_operating_entity_code, profitability_study, registration_entity, risk_unit, technical_follow_up, transverse_follow_up)
      }.toDF()
  }

  def prepareSpmelrpt(): DataFrame = {
    // Calculate:
    // - field [elr_status] and bring out the fields [codelr_map, rasoelr_map]
    val elrStatusFunctionUdf = udf(elrStatusFunction)

    dataLoader.load("rct_spmelrpt")
      .select("NUPERS_MAP", "CODELR_MAP", "RASOELR_MAP", "DATEFUS_MAP", "DATSUPL_MAP", "DATSUPE_MAP")
      .filter("CODELR_MAP is not null or NUPERS_MAP is not null")
      .filter("DATEFUS_MAP = '0000000000' and DATSUPL_MAP = '0000000000' and DATSUPE_MAP = '0000000000'")
      .withColumn("elr_status", elrStatusFunctionUdf(col("DATEFUS_MAP"), col("DATSUPL_MAP"), col("DATSUPE_MAP")))
  }

  def prepareSpmsrpmt(): (DataFrame, DataFrame) = {
    // Added fields [is_parent_company, le_group_id, le_subgroup_id]

    val spmsrpmt = dataLoader.load("rct_spmsrpmt")
      .select("NUMERO_PM", "NUMERO_SR", "CODE_TYPE_REGROUPEMENT", "CODE_ROLE_PM")

    // Remplacer la jointure.
    val spmsrpmt_gs = spmsrpmt.filter($"CODE_TYPE_REGROUPEMENT" === "GS")
      .select($"NUMERO_PM".as("NUMERO_PM_GS"), $"CODE_ROLE_PM", $"NUMERO_SR".as("le_group_id") /*, $"scoring_is_parent_company", $"is_parent_company"*/)

    val spmsrpmt_sg = spmsrpmt.filter($"CODE_TYPE_REGROUPEMENT" === "SG")
      .select($"NUMERO_PM".as("NUMERO_PM_SG"), $"NUMERO_SR".as("le_subgroup_id"))

    (spmsrpmt_gs, spmsrpmt_sg)
  }
}

case class Cdnsuivt(numero_pm: String, activity_follow_up_code: String, big_regulatory_risk_code: String, commercial_follow_up_code: String, list_follow_up_code: String, operational_center: String, pcru_code: String,
                    principal_operating_entity_code: String, profitability_study: String, registration_entity: String, risk_unit: String, technical_follow_up: String, transverse_follow_up: String)

object BuildLeBv {
  def elrStatusFunction() = (datefus_map: String, datsupl_map: String, datsupe_map: String) => {
    // The entry parameters are (datefus_map: String, datsupl_map: String, datsupe_map: String)
    var elr_status = "closed"
    if ("0000000000".equals(datefus_map) && "0000000000".equals(datsupl_map) && "0000000000".equals(datsupe_map)) {
      elr_status = "open"
    }
    elr_status
  }

  def dateFinRelationFunction() =
    (vars: Seq[String]) => {
      //date_fin_relation as var
      vars(0) match {
        case "00000000" => "open"
        case _ => "closed"
      }
    }

  def delegatedPcruFunction() =
  //the entry parameters are (cns1: String, cns2: String, cns3: String, cns4: String, cns5: String, indic1: String, indic2: String, indic3: String, indic4: String, indic5: String)
    (vars: Seq[String]) => {
      var delegated_pcru = "No"
      if (("01".equals(vars.head) && "1".equals(vars(5))) ||
        ("01".equals(vars(1)) && "1".equals(vars(6))) ||
        ("01".equals(vars(2)) && "1".equals(vars(7))) ||
        ("01".equals(vars(3)) && "1".equals(vars(8))) ||
        ("01".equals(vars(4)) && "1".equals(vars(9)))) {
        delegated_pcru = "Yes"
      }
      delegated_pcru
    }

  def scoringIsParentCompany() = (codeRolePm: String) => {
    codeRolePm match {
      case "MM" => "1"
      case _ => "0"
    }
  }

  def isParentCompany() = (codeRolePm: String) => {
    codeRolePm match {
      case "MM" => "true"
      case _ => "false"
    }
  }
}