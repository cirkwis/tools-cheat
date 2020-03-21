package com.socgen.bsc.cbs.businessviews.perle

import org.apache.spark.sql.{DataFrame, Row}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, Transformation}
import com.socgen.bsc.cbs.businessviews.perle.PerleOperation._

/**
  * Created by Minh-Hieu PHAM on 12/12/2016.
  */
class ArrayTransformation(target: String,
                          arrayFunc: Seq[Row] => String,
                          field: String) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val arrayUdf = udf(arrayFunc)
    df.withColumn(target, arrayUdf(col(field)))
  }
}

class BuildPerleBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "perle_le_bv"

  def applySpecificOperationToArray(target: String,
                                    func: (Seq[Row]) => String,
                                    field: String) = new ArrayTransformation(target, func, field)

  /**
    * UDF function: isArchived
    *
    **/
  def isArchived() = (vars: Seq[String]) => {
    val archivedStructure = vars(0)
    var result = ""
    try {
      if (archivedStructure.equals("0") == true) {
        result = "No"
      }
      else
        result = "Yes"
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }

    checkNull(result)
  }

  /**
    * UDF function: getRegistryName
    *
    **/
  def getRegistryName() = (vars: Seq[String]) => {
    val nineFirstCharacter = """(\d{3})\s(\d{3})\s(\d{3})\s""".r
    val rcsCode = vars(0)
    var result = ""
    try {
      result = nineFirstCharacter.replaceAllIn(rcsCode, "")
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }
    checkNull(result)
  }

  /**
    * UDF function: getInfo
    *
    * @param info : information to get from address structure (city, dest_country, headquarters_address,
    *             operational_address, physical_address)
    **/
  def getInfo(info: String) = (Addresses: Seq[Row]) => {

    /**
      * Function: getAdrFromRow
      * Description: Concatenation information from address structure
      *
      * @param adrFound : Array Address structure typed Row
      * @return concatenated string
      **/
    def getAdrFromRow(adrFound: Seq[Row]): String = {
      var result = ""
      if (adrFound.size != 0)
        result = result + (getFieldIfExist(adrFound(0), "StreetNumber") +
          " " + getFieldIfExist(adrFound(0), "StreetName1") +
          " " + getFieldIfExist(adrFound(0), "StreetName2") +
          " " + getFieldIfExist(adrFound(0), "ZipCode") +
          " " + getFieldIfExist(adrFound(0), "Town")).replace("null", "").replaceAll("\\s{2,}", " ").trim
      checkNull(result)
    }

    /**
      * Function: getCityOrCountry
      * Description: Getting information from address structure (city or country)
      *
      * @param adrFound : Array Address structure typed Row
      * @param info     : information to get from address structure (city or country)
      * @return concatenated string
      **/
    def getCityOrCountry(adrFound: Seq[Row], info: String): String = {
      var result = ""
      if (adrFound.size != 0)
        result = result + (info match {
          case "city" => getFieldIfExist(adrFound(0), "Town")
          case "dest_country" => getFieldIfExist(adrFound(0), "Country")
          case _ => "error"
        })
      checkNull(result)
    }

    /**
      * Function: Address filter by given TypeAddressCode
      * Note: This function is applied to filter function in list
      **/
    def filterAdrWithTypeAdrCode(code: String)(adr: Row): Boolean = {
      if (adr.schema.fields.map(a => a.name).contains("TypeAddress")) {
        if (adr.getAs[Row]("TypeAddress").getAs[String]("TypeAdressCode") != null)
          return (adr.getAs[Row]("TypeAddress").getAs[String]("TypeAdressCode").toString == code)
      }
      false
    }

    var result = ""

    val SSOC_cond = filterAdrWithTypeAdrCode("SSOC") _
    val OFF_cond = filterAdrWithTypeAdrCode("OFF") _
    val IMPL_cond = filterAdrWithTypeAdrCode("IMPL") _
    val PRINC_cond = filterAdrWithTypeAdrCode("PRINC") _

    if (Addresses != null) {
      var adrFound = Addresses.filter(SSOC_cond)

      if (adrFound.size == 0) {
        adrFound = Addresses.filter(OFF_cond)
      }

      val adrFoundIMPL = Addresses.filter(IMPL_cond)
      val adrFoundPRINC = Addresses.filter(PRINC_cond)

      result = result + (info match {
        case "city" => getCityOrCountry(adrFound, "city")
        case "dest_country" => getCityOrCountry(adrFound, "dest_country")
        case "headquarters_address" => getAdrFromRow(adrFound)
        case "operational_address" => getAdrFromRow(adrFoundPRINC)
        case "physical_address" => getAdrFromRow(adrFoundIMPL)
        case _ => "error"
      })
    }

    checkNull(result)
  }

  /**
    * Set of transformations to be realized
    **/
  val perleFinalTransformations = List(
    renameCol("USUALNAME", "acronym"),
    renameCol("NAFCODE", "ape_insee5_code"),
    renameCol("BRANCHCODE", "business_line_supervision_pole"),
    renameCol("REGISTRATIONDATE", "company_creation_date"),
    renameCol("STRUCKOFFREGISTERDATE", "date_of_dissolution"),
    renameCol("LEGALENTITYCODE_ID", "elr_code"),
    renameCol("FINANCIALSUPERVISOR", "financial_supervisor"),
    renameCol("LEGALFORMCODE", "legal_category_code"),
    renameCol("LEGALNAME", "legal_name"),
    renameCol("ACTIVITYSTATUSCODE", "legal_situation"),
    renameCol("LEGALENTITYIDENTIFIER", "lei_id"),
    renameCol("ACTIVITYSTATUSCODE", "legal_situation"),
    renameCol("MAGNITUDECODE", "magnitude_id"),
    renameCol("LEGALENTITYRESCODE", "res_id"),
    renameCol("SIRENCODE", "siren_identifier"),
    renameCol("SIRETCODE", "siret"),
    renameCol("LEGALENTITYSTPCODE", "stp_id"),
    renameCol("TYPESTRUCTURE", "structure_type_id"),
    renameCol("DEPARTMENTCODE", "supervision_department_id"),
    renameCol("SUBDEPARTMENTCODE", "supervision_sub_department_id"),
    renameCol("COMMERCIALNAME", "trade_as_name_1"),
    applySpecificOperation("archived", isArchived(), "ARCHIVEDSTRUCTURE"),
    applySpecificOperation("registry_name", getRegistryName(), "RCSCODE"),
    renameCol("ELRPERIMETERCODE", "elr_perimeter"),
    applySpecificOperationToArray("city", getInfo("city"), "ADDRESS"),
    applySpecificOperationToArray("dest_country", getInfo("dest_country"), "ADDRESS"),
    applySpecificOperationToArray("headquarters_address", getInfo("headquarters_address"), "ADDRESS"),
    applySpecificOperationToArray("operational_address", getInfo("operational_address"), "ADDRESS"),
    applySpecificOperationToArray("physical_address", getInfo("physical_address"), "ADDRESS")
  )

  override def process(): DataFrame = {
    val perleInitialSource = dataLoader.load("perle_legalentity")

    /** Apply final transformation */
    val result = perleInitialSource.applyOperations(perleFinalTransformations).filterDuplicates("duplicates_count", Seq("elr_code"))

    result
  }
}