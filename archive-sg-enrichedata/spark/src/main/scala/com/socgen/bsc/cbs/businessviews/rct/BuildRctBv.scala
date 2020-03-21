package com.socgen.bsc.cbs.businessviews.rct

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._

/**
  * The current BV consolidates the 5 BVs already developed on: com.socgen.bsc.cbs.businessviews.rct
  *
  * @param dataLoader
  */
class BuildRctBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val name = "rct_bv"
  override val frequency = Frequency.DAILY
  var schemaAvroPath = "avro/pm"

  val sqlContext = dataLoader.sqlContext

  override def process(): DataFrame = {
    var result: DataFrame = null

    // Load the principal BusinessView "rct_le_bv",
    // which will be enriched by the remaining RCT BusinessViews.
    result = dataLoader.load("rct_le_bv")

    // Enriched by "rct_group_bv"
    val rctGroupBv = dataLoader.load("rct_group_bv")
    result = result.join(rctGroupBv, result("le_group_id") === rctGroupBv("group_identifier"), "left_outer")

    // Enriched by "rct_subgroup_bv"
    val rctSubgroupBv = dataLoader.load("rct_subgroup_bv")
    result = result.join(rctSubgroupBv, result("le_subgroup_id") === rctSubgroupBv("subgroup_identifier"), "left_outer")

    // Enriched by RctNetworkRelationBV
    val rctNetworkRelationBv = prepareRctNetworkRelationBV()
    result = result.join(rctNetworkRelationBv, result("identifier") === rctNetworkRelationBv("network_le_identifier"), "left_outer")

    // Enrich rct_le_bv by RctIdmutBv
    val rctIdmutBv = prepareRctIdmutBv()
    val r = result.join(rctIdmutBv, result("identifier") === rctIdmutBv("idmut_le_identifier"), "left_outer")

    r.select("acquiring_le_identifier",
      "acronym",
      "activity_follow_up_code",
      "ape_insee5_code",
      "ape5_code",
      "big_regulatory_risk_code",
      "business_expiry_date",
      "client_type",
      "commercial_follow_up_code",
      "complement_dest_address",
      "delegated_pcru",
      "dest_country_code",
      "dun_id",
      "elr_code",
      "elr_legal_name",
      "elr_status",
      "end_relationship_reason_code",
      "flagprud",
      "group_number_legal_entities",
      "group_identifier",
      "group_name",
      "group_nationality_country",
      "group_nessg_code",
      "group_number_subgroups",
      "group_ultimate_parent_id",
      "identification_dest_address",
      "identifier",
      "le_group_id",
      "le_subgroup_id",
      "legal_category_code",
      "legal_name",
      "legal_situation_code",
      "lei_id",
      "list_follow_up_code",
      "magnitude_id",
      "naer5_code",
      "nessg_code",
      "network_relations",
      "operational_center",
      "other_idmut",
      "out_business_date",
      "payments_suspension_date",
      "pcru_code",
      "post_office_address",
      "principal_operating_entity_code",
      "profitability_study",
      "registration_entity",
      "relationship_end_date",
      "relationship_start_date",
      "relationship_status",
      "relationship_type_status",
      "risk_unit",
      "statutory_third_parties_cat_code",
      "stp_id",
      "subgroup_identifier",
      "subgroup_leader_entity_id",
      "subgroup_name",
      "subgroup_nationality_country",
      "subgroup_nessg_code",
      "subgroup_number_legal_entities",
      "swift_number",
      "technical_follow_up",
      "top_embargo",
      "transverse_follow_up",
      "company_creation_date",
      "headquarters_address",
      "legal_name_90c",
      "siren_identifier",
      "network_le_identifier",
      "idmut_le_identifier",
      "headquarters_address_complement_geo",
      "headquarters_address_lieu_dit",
      "headquarters_address_numero_voie",
      "headquarters_address_code_postal",
      "scoring_is_parent_company",
      "is_parent_company"
    ).filterDuplicates("duplicates_count", Seq("identifier"))

  }

  /**
    * Prepare the field "network_relations", which holds an array of networkRelation structure.
    * The networkRelation fields are defined on the "networkRelation.avsc" file.
    * The field "network_relations" is used to fill the ThirdPartyBV.
    *
    * @return
    * [[DataFrame]] with 2 fields:
    * First one: used for joining, and represents the RCT Third Party ID.
    * Second one: an array of NetworkRelation Struct.
    */
  def prepareRctNetworkRelationBV(): DataFrame = {
    // TODO: There is an interest on loading the field schema from avro file ?
    val networkRelationStructType = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "networkRelation.avsc")

    val networkRelationsType: StructType = StructType(Array(
      StructField("network_le_identifier", StringType, true),
      StructField("network_relations", ArrayType(networkRelationStructType), true)))

    val rddRow = dataLoader.load("rct_network_relation_bv")
      .filter("network_le_identifier IS NOT NULL")
      .rdd.groupBy(r => r.getAs[String]("network_le_identifier"))
      .map {
        case (networkLeIdentifier: String, networkRelations: Seq[Row]) => {
          // Select only a set of fields defined on the Avro Schema.
          val nrs: Seq[Row] = networkRelations.map {
            case row =>
              Row(networkRelationStructType.fieldNames.map(row.getAs[String](_)): _*)
          }
          Row(networkLeIdentifier, nrs)
        }

      }

    sqlContext.createDataFrame(rddRow, networkRelationsType)
  }

  /**
    * Prepare the fields:
    * - dun_id: String
    * - lei_id: String
    * - magnitude_id: String
    * - stp_id: String
    * - swift_number: String
    * - other_idmut: Map[String, String]
    * for each RCT Third Party ID.
    *
    * These ones are used to fill the ThirdPartyBV.
    *
    * @return
    * [[DataFrame]] with RCT Third Party ID "idmut_le_identifier" and the remaining fields listed above.
    */
  def prepareRctIdmutBv(): DataFrame = {
    val otherIdMutStructType = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "otherIdmut.avsc")

    val idmutStructType = StructType(
      StructField("idmut_le_identifier", StringType, true) ::
        StructField("dun_id", StringType, true) ::
        StructField("lei_id", StringType, true) ::
        StructField("magnitude_id", StringType, true) ::
        StructField("stp_id", StringType, true) ::
        StructField("swift_number", StringType, true) ::
        StructField("other_idmut", ArrayType(otherIdMutStructType), true) :: Nil)

    val result = dataLoader.load("rct_idmut_bv")
      .filter("idmut_le_identifier IS NOT NULL")
      .rdd.groupBy(r => r.getAs[String]("idmut_le_identifier"))
      .map {
        case (idmutLeIdentifier: String, rows: Seq[Row]) => {
          var mapTyVa: Map[String, String] = Map()

          rows.map {
            row => {
              mapTyVa += (row.getAs[String]("idmut_tymulti") -> row.getAs[String]("idmut_vamulti"))
            }
          }

          val dunId = mapTyVa.getOrElse("01664", null)
          val leiId = mapTyVa.getOrElse("COLEI", null)
          val magnitudeId = mapTyVa.getOrElse("COMAG", null)
          val stpId = mapTyVa.getOrElse("COSTP", null)
          val swiftNumber = mapTyVa.getOrElse("SWIFT", null)

          val otherIdmut = (mapTyVa - "01664" - "COLEI" - "COMAG" - "COSTP" - "SWIFT")
            .map(idMut => Row(idMut._1, idMut._2))

          Row(idmutLeIdentifier, dunId, leiId, magnitudeId, stpId, swiftNumber, otherIdmut)
        }
      }

    sqlContext.createDataFrame(result, idmutStructType)
  }

}