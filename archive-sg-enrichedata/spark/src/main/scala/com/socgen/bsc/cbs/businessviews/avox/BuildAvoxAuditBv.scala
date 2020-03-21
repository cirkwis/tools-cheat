package com.socgen.bsc.cbs.businessviews.avox

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, MultipleViews}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._

/**
  * Created by X149386 on 03/11/2016.
  */

class BuildAvoxAuditBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val frequency: Frequency.Value = Frequency.DAILY

  override val name = "avox_audit_bv"

  override def process(): DataFrame = {

    val transformations = List(
      renameCol("AVID", "avid"),
      renameCol("CLIENT_RECORD_ID", "client_record_id"),
      renameCol("DATA_SOURCE", "data_source"),
      renameCol("DATE_VERIFIED", "date_verified"),
      renameCol("FIELD_NAME", "field_name"),
      renameCol("DATA_SOURCE_URL", "source_url")
    )

    val df = dataLoader.load("avox_audit")
    val result = df.applyOperations(transformations)
    result
  }
}
