package com.socgen.bsc.cbs.businessviews.rct

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.common.BusinessView
import com.socgen.bsc.cbs.businessviews.common.Frequency


class BuildSubGroupBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "rct_subgroup_bv"

  val sqlContext = dataLoader.sqlContext

  import sqlContext.implicits._

  override def process(): DataFrame = {
    var result: DataFrame = null

    val spmsrpmt = prepareSpmsrpmt
    val spmstrgt = prepareSpmstrgt

    // Join Spmsrpmt & Spmstrgt
    result = spmsrpmt.join(spmstrgt,
      (spmsrpmt("NUMERO_SR") <=> spmstrgt("NUMERO")) &&
        (spmsrpmt("CODE_NATURE_SUIVI") <=> spmstrgt("CODE_NATURE_SUIVI")) &&
        (spmsrpmt("CODE_TYPE_REGROUPEMENT") <=> spmstrgt("CODE_TYPE_SR")), "left_outer")

    // Prepare the last view
    result.select($"NUMERO_SR".as("subgroup_identifier"),
      $"NOM".as("subgroup_name"),
      $"CODE_PAYS".as("subgroup_nationality_country"),
      $"CODE_NESSG".as("subgroup_nessg_code"),
      $"subgroup_leader_entity_id",
      $"subgroup_number_legal_entities")
      .distinct()
  }

  def prepareSpmsrpmt: DataFrame = {
    var result: DataFrame = null

    val spmsrpmt = dataLoader.load("rct_spmsrpmt")
      .select("NUMERO_SR", "CODE_TYPE_REGROUPEMENT", "CODE_NATURE_SUIVI", "NUMERO_PM", "CODE_ROLE_PM")
      .filter($"CODE_TYPE_REGROUPEMENT" === "SG")

    // add number legal entities field
    val spmsrpmtCount = spmsrpmt.select("NUMERO_SR").groupBy("NUMERO_SR").count()
      .select($"NUMERO_SR".as("NUMERO_SR_FOR_JOIN"), $"count".cast(StringType).as("subgroup_number_legal_entities"))

    result = spmsrpmt.join(spmsrpmtCount, $"NUMERO_SR" === spmsrpmtCount("NUMERO_SR_FOR_JOIN"), "left_outer")

    // add Leader Entity ID
    val spmsrpmtOnlyEL = spmsrpmt.filter($"CODE_ROLE_PM" === "EL")
      .select($"NUMERO_SR".as("NUMERO_SR_TO_DEL"), $"NUMERO_PM".as("subgroup_leader_entity_id"))

    result.join(spmsrpmtOnlyEL, $"NUMERO_SR" === spmsrpmtOnlyEL("NUMERO_SR_TO_DEL"), "left_outer")
  }

  def prepareSpmstrgt: DataFrame = {
    // Add spmstrgt columns
    dataLoader.load("rct_spmstrgt")
      .filter($"CODE_TYPE_SR" === "SG")
      .select("NUMERO", "CODE_TYPE_SR", "CODE_NATURE_SUIVI", "NOM", "CODE_PAYS", "CODE_NESSG")
  }
}
