package com.socgen.bsc.cbs.businessviews.thirdParties.config

import com.socgen.bsc.cbs.businessviews.thirdParties.config.DataSourceName.{DataSourceName, _}
import com.socgen.bsc.cbs.businessviews.thirdParties.models.ThirdType._

/**
  * Created by X153279 on 12/02/2017.
  */
object ThirdPartiesBvConfig {
  def globalMatchedBvFor(thirdType: ThirdType) = thirdType match {
    case PM => "global_matched_bv"
    case PP => "matching_pp_bv"
  }

  def outputViewName(thirdType: ThirdType) = thirdType match {
    case PM => "third_parties_bv"
    case PP => "pp_bv"
  }

  def outputMappingViewName(thirdType: ThirdType) = thirdType match {
    case PM => "third_parties_ids_bv"
    case PP => "pp_ids_bv"
  }

  def businessViewName(ds: DataSourceName) = ds match {
    case DUN => "dun_le_bv"
    case RCT => "rct_bv"
    case AVOX_FULL => "avox_le_bv"
    case INSEE => "insee_le_bv"
    case LEI => "lei_le_bv"
    case PERLE => "perle_le_bv"
    case INFOGREFFE => "infogreffe_le_bv"
    case IDQ => "idq_le_bv"
    case GRC_PM => "grc_pm_bv"
    case GRC_PP => "grc_pp_bv"
    case BDR => "bdr_le_bv"
    case PRIVALIA_PM => "privalia_pm_bv"
    case PRIVALIA_PP => "privalia_pp_bv"
    case CZ => "cz_le_bv"
    case SK => "sk_le_bv"
  }

  def keyForJoin(ds: DataSourceName) = ds match {
    case RCT => "identifier"
    case DUN => "dun_id"
    case AVOX_FULL => "identifier"
    case INSEE => "siret"
    case LEI => "lei_id"
    case PERLE => "elr_code"
    case INFOGREFFE => "hash_id"
    case IDQ => "identifier"
    case GRC_PM => "identifier"
    case GRC_PP => "identifier"
    case BDR => "identifier"
    case PRIVALIA_PM => "rct_identifier"
    case PRIVALIA_PP => "identifier"
    case CZ => "other_national_id"
    case SK => "other_national_id"
  }

  def bigMatchingColMappingName(ds: DataSourceName) = ds match {
    case RCT => "rct_bv"
    case DUN => "dun_le_bv"
    case AVOX_FULL => "avox_le_bv"
    case INSEE => "insee_le_bv"
    case LEI => "lei_le_bv"
    case PERLE => "perle_le_bv"
    case INFOGREFFE => "infogreffe_le_bv"
    case IDQ => "idq_le_bv"
    case GRC_PM => "grc_pm_bv"
    case GRC_PP => "grc_pp_bv"
    case BDR => "bdr_le_bv"
    case PRIVALIA_PM => "privalia_pm_bv"
    case PRIVALIA_PP => "privalia_pp_bv"
    case CZ => "cz_le_bv"
    case SK => "sk_le_bv"
  }

  def usedPrefix(ds: DataSourceName) = ds match {
    case RCT => "rct_"
    case DUN => "dun_"
    case AVOX_FULL => "avox_full_"
    case INSEE => "insee_"
    case LEI => "lei_"
    case PERLE => "perle_"
    case INFOGREFFE => "infogreffe_"
    case IDQ => "idq_"
    case GRC_PM => "grc_pm_"
    case GRC_PP => "grc_pp_"
    case BDR => "bdr_"
    case PRIVALIA_PM => "privalia_pm_"
    case PRIVALIA_PP => "privalia_pp_"
    case CZ => "cz_le_"
    case SK => "sk_le_"

  }

  /**
    * Values are used on:
    * - the ThirdPartyIDs mapping table.
    * - the Avro fields to specify the origin of the field value.
    * @param ds
    * @return
    */
  def usedNameForOutput(ds: DataSourceName) = ds match {
    case GRC_PM | GRC_PP => "grc"
    case PRIVALIA_PM | PRIVALIA_PP => "privalia"
    case _ => ds.toString
  }
}
