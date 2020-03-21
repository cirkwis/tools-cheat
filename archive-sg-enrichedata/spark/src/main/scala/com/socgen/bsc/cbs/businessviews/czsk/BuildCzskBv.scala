package com.socgen.bsc.cbs.businessviews.czsk

import com.socgen.bsc.cbs.businessviews.avox.Utils._
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.{DataFrameLoader, Frequency, MultipleViews, Transformation}
import com.socgen.bsc.cbs.businessviews.czsk.Utils._
import com.socgen.bsc.cbs.businessviews.thirdParties.avro.AvroFunctions
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Created by X156170 on 17/10/2017.
  */
class BuildCzskBv(override val dataLoader: DataFrameLoader) extends MultipleViews {
  override val frequency: Frequency.Value = Frequency.DAILY
  var schemaAvroPath = "avro"
  val sqlContext = dataLoader.sqlContext
  import sqlContext.implicits._


  override def process(): Map[String, DataFrame] = {
    val czsk = dataLoader.load("czsk")
    val cz = czsk.filter($"country" === "CZ")
    val sk = czsk.filter($"country" === "SK")
    Map("cz_le_bv" -> transform(cz,czOperations), "sk_le_bv" -> transform(sk,skOperations))
  }

  val operations : List[Transformation] = List(
    renameCol("town", "city"),
    applyMaper("nace_code", formatCodeNace , "business_activity_code"),
    renameCol("country", "dest_country_code"),
    mergeFields("headquarters_address", concatenate(" "), "designation", "address", "zipcode", "city"),
    renameCol("zipcode", "headquarters_address_code_postal"),
    renameCol("legal_name", "legal_name_90c"),

    identityCol("legal_form"),
    renameCol("registration_id", "other_national_id"),
    applyMaper("local_range_low", extractLowRange, "revenue_category"),
    applyMaper("local_range_high", extractHighRange, "revenue_category"),
    applyMaper("local_range_currency", extractCurrency, "revenue_category"),
    applyMaper("total_range_low", extractLowRange, "employee_category"),
    applyMaper("total_range_high", extractHighRange, "employee_category"),
    createStaticColumn("range_year", getCurrentYear.toString()),
    applyFilter("local_range_low", byThousand, "local_range_low", "local_range_currency"),
    applyFilter("local_range_high", byThousand, "local_range_high", "local_range_currency")
  )

  val skOperations = operations
  val czOperations = operations :+ renameCol("ESA_2010", "esa_2010")

  def createAnnualSales(df : DataFrame) : DataFrame = {
    val annualSalesSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/annualSales.avsc")

    val annualSalesType: StructType = StructType(Array(
      StructField("id", StringType, true),
      StructField("annual_sales", annualSalesSchema, true)))

    val annualSalesRddRow =
      df.select("other_national_id", "range_year", "local_range_low","local_range_high","local_range_currency").withColumnRenamed("range_year", "local_range_year")
        .rdd
        .map {
              row => Row(row.getAs[String]("other_national_id"), Row(annualSalesSchema.fieldNames.map(row.getAs[String](_)): _*))
        }
    sqlContext.createDataFrame(annualSalesRddRow, annualSalesType)
  }

  def createEmployees(df : DataFrame) : DataFrame = {
    val employeesSchema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/employees.avsc")
    val employeesType: StructType = StructType(Array(
      StructField("ids", StringType, true),
      StructField("employees", employeesSchema, true)))

    val rddRow =
      df.select("other_national_id", "range_year", "total_range_low","total_range_high").withColumnRenamed("range_year", "total_range_year")
        .rdd
        .map {
              row => Row(row.getAs[String]("other_national_id"), Row(employeesSchema.fieldNames.map(row.getAs[String](_)): _*))
        }
    sqlContext.createDataFrame(rddRow, employeesType)
  }


  def createNaceRev2(df: DataFrame): DataFrame = {
    val naceRev2Schema = AvroFunctions.getStructTypeFromAvroFile(schemaAvroPath, "common/naceRev2.avsc")

    val naceRev2Type: StructType = StructType(Array(
      StructField("id_national", StringType, true),
      StructField("nace_rev_2", naceRev2Schema, true)))

    val transformations = List(
      identityCol("other_national_id"),
      applyMaper("nace2_code_2", extract2FirstChar, "nace_code"),
      applyMaper("section", getSectionCode, "nace2_code_2"),
      applyMaper("division", getDivisionCode, "nace_code"),
      applyMaper("groupe", getGroupCode, "nace_code"),
      applyMaper("classe", getClassCode, "nace_code"),
      applyMaper("sous_classe", getSousClassCode, "nace_code"))


    val naceRev2RddRow =
      df.applyOperations(transformations)
        .rdd
        .map {
          row => Row(row.getAs[String]("other_national_id"), Row(naceRev2Schema.fieldNames.map(row.getAs[String](_)): _*))
        }
    sqlContext.createDataFrame(naceRev2RddRow, naceRev2Type)
  }

  def transform(df: DataFrame, operations: List[Transformation]): DataFrame = {

    val czskDf = df.applyOperations(operations)

    val annualSalesDf = createAnnualSales(czskDf)
    val employeesDf = createEmployees(czskDf)
    val naceRev2Df = createNaceRev2(czskDf)


    czskDf.join(annualSalesDf, czskDf.col("other_national_id") === annualSalesDf.col("id"),"left_outer")
      .join(employeesDf, czskDf.col("other_national_id") === employeesDf.col("ids"),"left_outer")
      .join(naceRev2Df, czskDf.col("other_national_id") === naceRev2Df.col("id_national"),"left_outer")
      .drop("local_range_low")
      .drop("local_range_high")
      .drop("local_range_currency")
      .drop("range_year")
      .drop("total_range_low")
      .drop("total_range_high")
      .drop("id_national")
      .drop("id")
      .drop("ids")
      .filterDuplicates("duplicates_count", Seq("other_national_id"))
  }

}
