package com.socgen.bsc.cbs.businessviews.bdr

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import com.socgen.bsc.cbs.businessviews.common.CommonUdf.concatenate
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer.{mergeFields, _}
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, Transformation}

import scala.collection.mutable

/**
  * Created by X150102 on 27/03/2017.
  */
class BuildBdrLeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency = Frequency.DAILY
  override val name = "bdr_le_bv"

  override def process(): DataFrame = {
    val datasetParty = selectSource(dataLoader, "bdr_party", fieldsParty)

    val datasetPartyWithR = joinWithPartyRegulatoryStatus(
      datasetParty,
      selectSource(dataLoader, "bdr_partyregulatorystatus", fieldsPartyRegulatoryStatus)
    )

    val datasetPartyWithRL = joinWithPartyLegalEntity(
      datasetPartyWithR,
      selectSource(dataLoader, "bdr_legalentity", fieldsPartyLegalEntity)
    )

    val datasetPartyWithRLF = joinWithPartyFollowUpClass(
      datasetPartyWithRL,
      selectSource(dataLoader, "bdr_followupclass", fieldsPartyFollowUpClass)
    )

    val datasetPartyWithRLFA = joinWithPartyAdresses(
      datasetPartyWithRLF,
      selectSource(dataLoader, "bdr_partyaddresses", fieldsPartyAdresses)
    )

    val result = datasetPartyWithRLFA.applyOperations(transformations)

    /*Add empty column because we don't receive the table yet*/
    result.withColumn("follow_up_Sector_id", lit(null).cast(StringType))
      .withColumn("follow_up_Sector_label", lit(null).cast(StringType))
      .withColumn("follow_up_Sector_name", lit(null).cast(StringType))
      .withColumn("primary_role_name", lit(null).cast(StringType))
      .withColumn("primary_role_shortname", lit(null).cast(StringType))
      .withColumn("partyIndividualIndicator", lit(null).cast(StringType))
      .withColumn("client_type", lit(null).cast(StringType))
  }

  def applySpecificOperationToArray(target: String,
                                    func: (Seq[Seq[String]]) => String,
                                    fields: String*) = new ArrayTransformation(target, func, fields: _*)

  def selectSource(dfl: DataFrameLoader, source: String, fields: List[String]): DataFrame = {
    dfl.load(source).select(fields.map(col): _*)
  }

  def joinWithPartyRegulatoryStatus(mainTable: DataFrame, partyRegulatoryStatusSample: DataFrame): DataFrame = {
    val rigthTab = partyRegulatoryStatusSample.applyOperations(List(renameCol("partyId_id", "bdr_partyregulatorystatus_partyId_id")), true)
    mainTable.join(rigthTab, col("partyId_id") === col("bdr_partyregulatorystatus_partyId_id"), "left_outer")
  }

  def joinWithPartyLegalEntity(mainTable: DataFrame, partyLegalEntitySample: DataFrame): DataFrame = {
    val rigthTab = partyLegalEntitySample.applyOperations(List(renameCol("legalEntityId_id", "bdr_legalentity_legalEntityId_id")), true)
    mainTable.join(rigthTab, col("legalEntityId_id") === col("bdr_legalentity_legalEntityId_id"), "left_outer")
  }

  def joinWithPartyFollowUpClass(mainTable: DataFrame, partyFollowUpClass: DataFrame): DataFrame = {
    val rigthTab = partyFollowUpClass.applyOperations(List(renameCol("followUpClassId_id", "bdr_followupclass_followUpClassId_id")), true)
    mainTable.join(rigthTab, col("followUpClassId_id") === col("bdr_followupclass_followUpClassId_id"), "left_outer")
  }

  def joinWithPartyAdresses(mainTable: DataFrame, partyAdresses: DataFrame): DataFrame = {
    val rigthTab = partyAdresses.applyOperations(List(renameCol("partyId_id", "bdr_partyaddresses_partyId_id")), true)
    mainTable.join(rigthTab, col("partyId_id") === col("bdr_partyaddresses_partyId_id"), "left_outer")
  }

  val AddressFields = List(
    "partyRegisteredAddress_addressHeading",
    "partyRegisteredAddress_addressHeadingComplement",
    "partyRegisteredAddress_streetName",
    "partyRegisteredAddress_postalCode",
    "partyRegisteredAddress_cityName",
    "partyRegisteredAddress_stateName",
    "partyRegisteredAddress_addressLocalityComplement")

  val fieldsParty = List(
    "partyId_id",
    "partyMnemonic_code",
    "partyFxTradingName",
    "partyLegalName",
    "partyShortName",
    "partyPrimaryRoleMnemonic",
    "partyRoleSubClass",
    "partySecondaryRoleMnemonic",
    "activeIndicator",
    "inactivationInputComment_commentInfo",
    "inactivationOutputComment_commentInfo",
    "NAECode_code",
    "activitySector",
    "NAERevisedCode_code",
    "kycStatusCountry",
    "kycLocalStatus",
    "kycLastReviewDate",
    "authorizationToDeal",
    "approvedTradingType",
    "BlackListIndicator",
    "partyNationalityCountry",
    "partyResidenceCountry",
    "KYCMainCountryIndicator",
    "localOfficeCode_code",
    "partySubAccountIndicator",
    "localOfficeCode_codingScheme",
    "cityName",
    "partyInternalCategory",
    "partyRegulationIndicator",
    "partyRegulatorMnemonic",
    "partyListedIndicator",
    "listedCompanySubsidiaryIndicator",
    "partyParentCompanyName",
    "marketId_id",
    "partyParentCompanyId_id",
    "localApplicationPartyCode_code",
    "monetaryResidenceIndicator",
    "localApplicationPartyCode_code",
    "EECIndicator",
    "fiscalResidenceIndicator",
    "partyFamily",
    "KYCFirstValidationStatus",
    "legalEntityIdentifierId_id",
    "legalEntityId_id",
    "partyPrimaryRoleId_id",
    "followUpClassId_id",
    "partyIndustryCode_code",
    "partyIndustryCode_codingScheme",
    "partyTypeCode_code",
    "headOfficeIndicator"

  )
  val fieldsPartyRegulatoryStatus = List(
    "partyRegulatoryStatusInfo_bookingCountry",
    "partyFinancialInstitutionIdentifierStatus",
    "partyRegulatoryStatusInfo_leadingCountryIndicator",
    "detailedStatusCode_code",
    "partyRegulatoryStatusInfo_detailedStatusValidationDate",
    "partyRegulatoryStatusInfo_detailedStatusExpiryDate",
    "partyId_id"
  )
  val fieldsPartyLegalEntity = List(
    "legalEntityId_id",
    "liaisonStructureId_id",
    "legalEntityLocationInfo_legalEntityNationalityCountry",
    "regulatoryCategoryCode_code",
    "legalEntityMnemonic_code",
    "legalEntityLongName",
    "legalEntityLegalName",
    "goldenId",
    "legalEntityLocationInfo_assetLocationCountry",
    "sensitivityLevel_code",
    "legalEntitySIRENCode_code",
    "legalEntityGroupCode_code",
    "legalEntityGroupCode_codingScheme",
    "externalRegistrationId_id",
    "externalRegistrationId_idScheme"

  )
  val fieldsPartyFollowUpClass = List(
    "followUpClassId_id",
    "followUpClassName",
    "followUpClassLabel")
  val fieldsPartyAdresses = List(
    "partyId_id",
    "partyRegisteredAddress_addressHeading",
    "partyRegisteredAddress_addressHeadingComplement",
    "partyRegisteredAddress_streetName",
    "partyRegisteredAddress_postalCode",
    "partyRegisteredAddress_cityName",
    "partyRegisteredAddress_stateName",
    "partyRegisteredAddress_addressLocalityComplement"
  )

  val transformations = List(
    renameCol("followUpClassId_id", "follow_up_class_id"),
    renameCol("followUpClassLabel", "follow_up_class_label"),
    renameCol("followUpClassName", "follow_up_class_name"),
    renameCol("goldenId", "golden_id"),
    renameCol("legalEntityId_id", "bdr_id"),
    renameCol("legalEntityLegalName", "legalEntity_legal_name"),
    renameCol("legalEntityLocationInfo_assetLocationCountry", "assets_country_localisation"),
    renameCol("legalEntityLocationInfo_legalEntityNationalityCountry", "country_le"),
    renameCol("legalEntityLongName", "legalEntity_short_name"),
    renameCol("legalEntityMnemonic_code", "legalEntity_mnemonic_code"),
    renameCol("legalEntitySIRENCode_code", "siren_identifier"),
    renameCol("liaisonStructureId_id", "structure_link"),
    renameCol("regulatoryCategoryCode_code", "ctr_code"),
    renameCol("sensitivityLevel_code", "sensitivity_rank"),
    renameCol("activeIndicator", "active"),
    renameCol("activitySector", "activity_sector"),
    renameCol("approvedTradingType", "authorization_to_deal_approve_trading_type"),
    renameCol("authorizationToDeal", "authorization_to_deal_auth_deal"),
    renameCol("BlackListIndicator", "blacklisted"),
    renameCol("cityName", "city"),
    renameCol("EECIndicator", "complement"),
    renameCol("fiscalResidenceIndicator", "tax_res"),
    renameCol("inactivationInputComment_commentInfo", "input_inactiv_reason"),
    renameCol("inactivationOutputComment_commentInfo", "output_inactiv_reason"),
    renameCol("KYCFirstValidationStatus", "statut_kyc"),
    renameCol("kycLastReviewDate", "authorization_to_deal_last_review_date"),
    renameCol("kycLocalStatus", "authorization_to_deal_local_status"),
    renameCol("KYCMainCountryIndicator", "kyc_country_risk_indicator"),
    renameCol("kycStatusCountry", "authorization_to_deal_country_EL"),
    renameCol("legalEntityIdentifierId_id", "lei_id"),
    renameCol("listedCompanySubsidiaryIndicator", "subsidiary"),
    copyCol("localApplicationPartyCode_code", "app_code", true),
    copyCol("localApplicationPartyCode_code", "ref_local_id"),
    renameCol("localOfficeCode_code", "lpl_code"),
    renameCol("localOfficeCode_codingScheme", "managing_entity"),
    renameCol("marketId_id", "place_of_quota"),
    renameCol("monetaryResidenceIndicator", "monetary_res"),
    renameCol("NAECode_code", "nae_code"),
    renameCol("NAERevisedCode_code", "nae_rev_code"),
    renameCol("partyFamily", "typology"),
    renameCol("partyFxTradingName", "trader_name"),
    renameCol("partyId_id", "identifier"),
    renameCol("partyLegalName", "legal_name"),
    renameCol("partyListedIndicator", "listed"),
    renameCol("partyMnemonic_code", "mnemonic_code"),
    renameCol("partyNationalityCountry", "nat_country"),
    renameCol("partyParentCompanyId_id", "parent_company_id"),
    renameCol("partyParentCompanyName", "parent_company_name"),
    renameCol("partyPrimaryRoleMnemonic", "primary_role_mnemonic"),
    renameCol("partyRegulationIndicator", "kyc_regulated"),
    renameCol("partyRegulatorMnemonic", "regulator_mnemonic"),
    renameCol("partyResidenceCountry", "res_country"),
    renameCol("partyRoleSubClass", "subclass"),
    renameCol("partySecondaryRoleMnemonic", "secondary_role_mnemonic"),
    renameCol("partyShortName", "short_name"),
    renameCol("partySubAccountIndicator", "sub_account"),
    renameCol("partyInternalCategory", "ent_fund"),
    renameCol("detailedStatusCode_code", "fatca_detailed_status"),
    renameCol("partyRegulatoryStatusInfo_detailedStatusExpiryDate", "fatca_detailed_expiry_date"),
    renameCol("partyRegulatoryStatusInfo_detailedStatusValidationDate", "fatca_detailed_validation_date"),
    renameCol("partyRegulatoryStatusInfo_leadingCountryIndicator", "fatca_leading_country_indicator"),
    renameCol("partyFinancialInstitutionIdentifierStatus", "fatca_status"),
    renameCol("partyRegulatoryStatusInfo_bookingCountry", "fatca_booking_country"),
    applySpecificOperationToArray("isin_code", BuildBdrLeBv.isin_code(), "partyIndustryCode_code", "partyIndustryCode_codingScheme"),
    applySpecificOperationToArray("siret_id", BuildBdrLeBv.siren_id(), "partyIndustryCode_code", "partyIndustryCode_codingScheme"),
    applySpecificOperationToArray("swift_number", BuildBdrLeBv.swift_id(), "partyIndustryCode_code", "partyIndustryCode_codingScheme"),

    applySpecificOperation("rct_id", BuildBdrLeBv.rct_id(), "legalEntityGroupCode_code", "legalEntityGroupCode_codingScheme"),
    applySpecificOperation("dun_id", BuildBdrLeBv.dun_number(), "externalRegistrationId_id", "externalRegistrationId_idScheme"),

    applySpecificOperation("is_headquarter", BuildBdrLeBv.is_headquarter(), "headOfficeIndicator"),
    mergeFields("adresses", concatenate(" "), AddressFields: _*)
  )
}

object BuildBdrLeBv {
  /** BDR Standard 1 algorithm */
  def checkFieldsVal(fieldValToReturn: String, fieldValToControl: String, refVal: String): String = {
    var result = ""
    if (refVal.equals(fieldValToControl)) {
      result = fieldValToReturn
    } else {
      result = null
    }
    result
  }
  /** BDR Standard 2 algorithm */
  def checkFieldsValInArray(vars: Seq[Seq[String]], refVal: String): String = {
    var result = ""

    if (vars.size == 2) {
      val partyIndustryCode_code = vars(0)
      val partyIndustryCode_codingScheme = vars(1)

      if (partyIndustryCode_code != null && partyIndustryCode_codingScheme != null && partyIndustryCode_code.size == partyIndustryCode_codingScheme.size) {
        if (partyIndustryCode_codingScheme.contains(refVal)) {
          result = partyIndustryCode_code(partyIndustryCode_codingScheme.indexOf(refVal))
        } else {
          result = null
        }
      } else {
        //TODO :[Some field is null] This case must never hapend. If it's hapen we musth log to trace this error
        result = null
      }
    } else {
      //TODO :[Not Have 2 fields] This case must never hapend. If it's hapen we musth log to trace this error
      result = null
    }
    result
  }

  def rct_id() = (vars: Seq[String]) => {
    checkFieldsVal(vars(0), vars(1), "RCT")
  }

  def dun_number() = (vars: Seq[String]) => {
    checkFieldsVal(vars(0), vars(1), "DUNSnumber")
  }

  def isin_code() = (vars: Seq[Seq[String]]) => {
    checkFieldsValInArray(vars, "ISIN")
  }

  def siren_id() = (vars: Seq[Seq[String]]) => {
    checkFieldsValInArray(vars, "SIRET")
  }

  def swift_id() = (vars: Seq[Seq[String]]) => {
    checkFieldsValInArray(vars, "BIC")
  }

  def is_headquarter() = (vars: Seq[String]) => {
    val varHeadOfficeIndicator = vars(0)
    var result = "false"

    varHeadOfficeIndicator match {
      case null => result = null
      case "true" => result = "Y"
      case "false" => result = "N"
    }
    result
  }
}

class ArrayTransformation(target: String,
                          arrayFunc: Seq[Seq[String]] => String,
                          fields: String*) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val arrayUdf = udf(arrayFunc)
    val cols = fields.map(col)
    df.withColumn(target, arrayUdf(array(cols: _*)))
  }
}
