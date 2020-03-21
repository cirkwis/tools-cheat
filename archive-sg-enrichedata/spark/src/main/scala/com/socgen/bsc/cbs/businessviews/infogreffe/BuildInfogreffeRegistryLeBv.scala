package com.socgen.bsc.cbs.businessviews.infogreffe

import org.apache.spark.sql.DataFrame
import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._

/**
  * Created by X156170 on 12/12/2016.
  */
class BuildInfogreffeRegistryLeBv(override val dataLoader: DataFrameLoader) extends BusinessView {
  override val name = "infogreffe_Registry_LE_BV"
  override val frequency = Frequency.MONTHLY


  override def process(): DataFrame = {
    val dataFrame = dataLoader.load("infogreffe_liste_des_greffes")
    val listOfTransformations = List(
      renameCol("numero_1", "registry_id"),
      renameCol("site_web", "registry_local_site"),
      renameCol("greffe", "registry_name")
    )

    dataFrame.applyOperations(listOfTransformations)
  }

}
