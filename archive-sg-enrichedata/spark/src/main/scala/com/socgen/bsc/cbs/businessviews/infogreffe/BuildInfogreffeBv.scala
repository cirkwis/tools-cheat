package com.socgen.bsc.cbs.businessviews.infogreffe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._

/**
  * Created by X156170 on 13/01/2017.
  */
class BuildInfogreffeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.MONTHLY
  override val name = "infogreffe_le_bv"


  override def process(): DataFrame = {

    val deregisteredLeBv = new BuildInfogreffeDeregisteredLeBv(dataLoader).process()
    val keyfiguresBv = new BuildInfogreffeKeyfiguresBv(dataLoader).process()
    val registeredLeBv = new BuildInfogreffeRegisteredLeBv(dataLoader).process()
    val registryLeBv = new BuildInfogreffeRegistryLeBv(dataLoader).process()
    val udfConcatenate = udf(concatenate("_"))
    registeredLeBv.withColumnRenamed("legal_situation", "legal_situation_")
      .cleanJoin(deregisteredLeBv, "siren", "outer", tmpSiren())
      .cleanJoin(keyfiguresBv, "siren", "outer", tmpSiren())
      .join(registryLeBv.withColumnRenamed("registry_name", "registry_name_"), col("registry_name").equalTo(col("registry_name_")), "left_outer")
      .drop("registry_name_")
      .cleanUpdate("legal_situation", updateLegalSituation())
      .withColumn("hash_id",udfConcatenate(array(List("siren","siret").map(col):_*)))
  }

  implicit class ExtraDataframe(df: DataFrame) { //TODO
  def cleanJoin(rightDf: DataFrame, keyColumn: String, joinType: String, function: (String, String) => String): DataFrame = {
    val udfFunction = udf(function)
    val tmpKeyColumn = keyColumn + "_"
    val aggregatedColumn = tmpKeyColumn + "_"
    df.join(rightDf.withColumnRenamed(keyColumn, tmpKeyColumn), col(keyColumn).equalTo(col(tmpKeyColumn)), joinType)
      .withColumn(aggregatedColumn, udfFunction(col(keyColumn), col(tmpKeyColumn)))
      .drop(keyColumn)
      .drop(tmpKeyColumn)
      .withColumnRenamed(aggregatedColumn, keyColumn)
  }

    def cleanUpdate(columnToUpdate: String, updateFunction: (String, String) => String): DataFrame = {
      val udfFunction = udf(updateFunction)
      val column2 = columnToUpdate + "_"
      val tmpColumn = column2 + "_"
      df.withColumn(tmpColumn, udfFunction(col(column2), col(columnToUpdate)))
        .drop(columnToUpdate)
        .drop(column2)
        .withColumnRenamed(tmpColumn, columnToUpdate)
    }
  }

  def updateLegalSituation() = {
    (s1: String, s2: String) => {
      if (s1 == "Active" && s2 == "Struck off") "Struck off"
      else if (s1 == null && s2 == "Struck off") "Struck off"
      else if (s1 == "Active" && s2 == null) "Active"
      else null
    }
  }

  def tmpSiren() = {
    (s1: String, s2: String) => {
      if (s1 == null & s2 == null) null
      else if (s1 == null) s2
      else if (s2 == null) s1
      else s1
    }
  }
}
