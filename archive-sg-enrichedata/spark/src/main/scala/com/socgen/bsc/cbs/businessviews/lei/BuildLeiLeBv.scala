package com.socgen.bsc.cbs.businessviews.lei

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}

/**
  * Created by X149386 on 03/11/2016.
  */

class BuildLeiLeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency: Frequency.Value = Frequency.DAILY
  override val name = "lei_le_bv"

  override def process(): DataFrame = {

    val headQuartersAddressFields = List("HeadquartersAddress_Line1","HeadquartersAddress_Line2",
      "HeadquartersAddress_Line3","HeadquartersAddress_Line4","HeadquartersAddress_PostalCode",
      "HeadquartersAddress_City")

    val legalAddressFields = List("LegalAddress_Line1","LegalAddress_Line2","LegalAddress_Line3", "LegalAddress_Line4",
      "LegalAddress_PostalCode","LegalAddress_City")

    val transformations = List(
      copyCol("HeadquartersAddress_City", "city"),
      copyCol("LegalAddress_Country", "dest_country_code"),
      mergeFields("headquarters_address",concatenate(" ") , headQuartersAddressFields:_*),
      mergeFields("legal_address",concatenate(" ") , legalAddressFields:_*),
      copyCol("LegalForm", "legal_form"),
      renameCol("LegalName", "legal_name_90c"),
      copyCol("RegistrationStatus", "legal_situation"),
      renameCol("LEI_ID", "lei_id"),
      renameCol("ManagingLOU", "lei_lou_id"),
      renameCol("RegistrationStatus", "lei_status"),
      applyFilter("other_national_id", customFilter(List("FR_1", "FR001"), includeFilter = false), "BusinessRegisterEntityID", "BusinessRegisterEntityID_Register"),
      renameCol("EntityStatus", "relationship_status"),
      applyFilter("siren", customFilter(List("FR_1", "FR001"), includeFilter = true), "BusinessRegisterEntityID", "BusinessRegisterEntityID_Register")
    )

    val df = dataLoader.load("lei")
    val result = df.applyOperations(transformations).filterDuplicates("duplicates_count", Seq("lei_id"))
    result
  }
}