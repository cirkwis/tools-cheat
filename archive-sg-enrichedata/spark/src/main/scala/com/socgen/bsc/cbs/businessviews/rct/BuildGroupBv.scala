package com.socgen.bsc.cbs.businessviews.rct

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.common.BusinessView
import com.socgen.bsc.cbs.businessviews.common.Frequency

class BuildGroupBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "rct_group_bv"

  val sqlContext = dataLoader.sqlContext

  import sqlContext.implicits._

  override def process(): DataFrame = {
    var result: DataFrame = null

    // Join Spmsrpmt & Spmsrsrt
    val spmsrpmt = prepareSpmsrpmt()
    val spmsrsrt = prepareSpmsrsrt()
    result = spmsrpmt.join(spmsrsrt, $"NUMERO_SR" === spmsrsrt("numero_structure_groupe"), "left_outer")

    // Join the result & Spmstrgt
    val spmstrgt = prepareSpmstrgt()
    result = result.join(spmstrgt,
      (result("NUMERO_SR") <=> spmstrgt("NUMERO")) &&
        (result("CODE_NATURE_SUIVI") <=> spmstrgt("CODE_NATURE_SUIVI")) &&
        (result("CODE_TYPE_REGROUPEMENT") <=> spmstrgt("CODE_TYPE_SR")), "left_outer")

    // last selection before persisting business view result
    result.select($"NUMERO_SR".as("group_identifier"),
      $"NOM".as("group_name"),
      $"CODE_PAYS".as("group_nationality_country"),
      $"CODE_NESSG".as("group_nessg_code"),
      $"group_ultimate_parent_id",
      $"group_number_legal_entities",
      $"group_number_subgroups"
    ).distinct()
  }

  def prepareSpmsrpmt(): DataFrame = {
    var result: DataFrame = null

    val spmsrpmt = dataLoader.load("rct_spmsrpmt")
      .select("NUMERO_SR", "CODE_TYPE_REGROUPEMENT", "CODE_NATURE_SUIVI", "NUMERO_PM", "CODE_ROLE_PM")
      .filter($"CODE_TYPE_REGROUPEMENT" === "GS")

    // add number legal entities field
    val spmsrpmtCountEntities = spmsrpmt.select("NUMERO_SR").groupBy("NUMERO_SR").count()
      .select($"NUMERO_SR".as("NUMERO_SR_FOR_JOIN"), $"count".cast(StringType).as("group_number_legal_entities"))

    result = spmsrpmt.join(spmsrpmtCountEntities, $"NUMERO_SR" === spmsrpmtCountEntities("NUMERO_SR_FOR_JOIN"), "left_outer")

    // add Ultimate Parent ID
    val spmsrpmtUltimateParentId = spmsrpmt.filter($"CODE_ROLE_PM" === "MM")
      .select($"NUMERO_SR".as("NUMERO_SR_FOR_JOIN2"), $"NUMERO_PM".as("group_ultimate_parent_id"))

    result.join(spmsrpmtUltimateParentId, $"NUMERO_SR" === spmsrpmtUltimateParentId("NUMERO_SR_FOR_JOIN2"), "left_outer")
  }

  def prepareSpmsrsrt(): DataFrame = {
    // Add sub-group count per group
    dataLoader.load("rct_spmsrsrt")
      .select("NUMERO_STRUCTURE_GROUPE", "CODE_TYPE_REGROUPEMENT_GR", "NUMERO_STRUCTURE_SOUS_GRP", "CODE_TYPE_REGROUPEMENT_SOUS")
      .filter($"CODE_TYPE_REGROUPEMENT_GR" === "GS" && $"CODE_TYPE_REGROUPEMENT_SOUS" === "SG")
      .select("NUMERO_STRUCTURE_GROUPE")
      .groupBy("NUMERO_STRUCTURE_GROUPE")
      .count()
      .select($"NUMERO_STRUCTURE_GROUPE".as("numero_structure_groupe"), $"count".cast(StringType).as("group_number_subgroups"))
  }

  def prepareSpmstrgt(): DataFrame = {
    // add spmstrgt columns
    dataLoader.load("rct_spmstrgt")
      .select("NUMERO", "CODE_TYPE_SR", "CODE_NATURE_SUIVI", "NOM", "CODE_PAYS", "CODE_NESSG")
      .filter($"CODE_TYPE_SR" === "GS")
  }
}