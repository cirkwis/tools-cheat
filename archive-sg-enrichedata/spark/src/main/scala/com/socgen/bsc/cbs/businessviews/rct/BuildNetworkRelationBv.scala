package com.socgen.bsc.cbs.businessviews.rct

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.BusinessView
import com.socgen.bsc.cbs.businessviews.common.Frequency
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._


class BuildNetworkRelationBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "rct_network_relation_bv"

  def dateFinRelationFunction =
    (vars: Seq[String]) => {
      //date_fin_relation as var
      vars(0) match {
        case "00000000" => "open"
        case _ => "closed"
      }
    }

  override def process(): DataFrame = {
    val transformations = List(
      renameCol("SIGLE", "network_acronym"),
      mergeFields("network_address", concatenate(" "), "COMPLEMENT_GEO", "NUMERO_VOIE", "LIEU_DIT", "CODE_POSTAL_LOCALITE"),
      renameCol("TYPE_CLIENT", "network_client_type"),
      renameCol("DENOMINATION_COMM_90", "network_comm_denomination_90c"),
      renameCol("DENOMINATION_COMMERCIALE", "network_commercial_denomination"),
      renameCol("CODE_PAYS_DEST", "network_dest_country_code"),
      renameCol("IDENTIFIANT_ES", "network_es_identifier"),
      renameCol("FLAGPRUD", "network_flagprud"),
      renameCol("IDENTIFIANT_GENERIQ_RES", "network_generic_network_identifier"),
      renameCol("ENTITE_IMMATRICULATION", "network_immatriculation_entity"),
      renameCol("NUMERO_PM", "network_le_identifier"),
      renameCol("NUMERO_RESEAU", "network_network_number"),
      applySpecificOperation("network_relationship_status", dateFinRelationFunction, "DATE_FIN"),
      renameCol("DATE_FIN", "network_end_date"),
      renameCol("STATUT_TYPE_RELATION", "network_relationship_type_status"),
      renameCol("DATE_ENTREE", "network_start_date"),
      renameCol("CODE_CAT_TIERS_REGLEM", "network_statutory_third_parties_cat_code"),
      renameCol("TOP_EMBARGO", "network_top_embargo")
    )

    val df = dataLoader.load("rct_cdnrdret")
    df.applyOperations(transformations)
  }
}