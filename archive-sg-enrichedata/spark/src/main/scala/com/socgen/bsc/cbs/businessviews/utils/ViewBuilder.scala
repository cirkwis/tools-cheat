package com.socgen.bsc.cbs.businessviews.utils

import com.socgen.bsc.cbs.businessviews.avox.{BuildAvoxAuditBv, BuildAvoxLeBv}
import com.socgen.bsc.cbs.businessviews.bdr.BuildBdrLeBv
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader
import com.socgen.bsc.cbs.businessviews.grc.{BuildGrcPmBv, BuildGrcPpBv}
import com.socgen.bsc.cbs.businessviews.dun.BuildDunLeBv
import com.socgen.bsc.cbs.businessviews.insee.BuildInseeLeBv
import com.socgen.bsc.cbs.businessviews.idq.BuildIdqBv
import com.socgen.bsc.cbs.businessviews.infogreffe.BuildInfogreffeBv
import com.socgen.bsc.cbs.businessviews.lei.BuildLeiLeBv
import com.socgen.bsc.cbs.businessviews.matching_pp.BuildMatchingPpBv
import com.socgen.bsc.cbs.businessviews.perle.BuildPerleBv
import com.socgen.bsc.cbs.businessviews.privalia.{BuildPrivaliaPmBv, BuildPrivaliaPpBv}
import com.socgen.bsc.cbs.businessviews.rct.{BuildRctBv, _}
import com.socgen.bsc.cbs.businessviews.czsk.BuildCzskBv
import com.socgen.bsc.cbs.businessviews.grc.delta._

/**
  * Created by X149386 on 16/11/2016.
  */
class ViewBuilder(views: Seq[String], dataLoader: DataFrameLoader) {

  def buildViews(): Unit = {

    for (view: String <- views) {
      view match {
        case "rct" => buildRctBusinessViews()
        case "avox" => buildAvoxBusinessViews()
        case "lei" => buildLeiBusinessViews()
        case "insee" => buildInseeBusinessViews()
        case "grc_pm" => buildGrcPmBusinessViews()
        case "dun" => buildDunBusinessViews()
        case "perle_legalentity" => buildPerleBusinessViews()
        case "infogreffe" =>buildInfogreffeBusinessViews()
        case "grc_pp" => buildGrcPpBusinessViews()
        case "idq" => buildIdqBusinessViews()
        case "bdr" => buildBdrBusinessViews()
        case "matching_pp" => buildMatchingPpBusinessViews()
        case "privalia" => buildPrivaliaBusinessViews()
        case "czsk" => buildCzskBusinesViews()
        case "grcdelta" => buildGrcDeltaBusinessViews()
        case _ =>
      }
    }
  }

  def buildMatchingPpBusinessViews() : Unit = {
    new BuildMatchingPpBv(dataLoader).execute("json")
  }

  def buildGrcDeltaBusinessViews() : Unit = {
    new BuildGrcLienStock(dataLoader).execute()
    new BuildGrcLienTiersPresStock(dataLoader).execute()
    new BuildGrcPmStock(dataLoader).execute()
    new BuildGrcPpStock(dataLoader).execute()
    new BuildGrcPrestaStock(dataLoader).execute()
  }

  def buildCzskBusinesViews() : Unit = {
    new BuildCzskBv(dataLoader).execute()
  }

  def buildIdqBusinessViews(): Unit = {
    new BuildIdqBv(dataLoader).execute()
  }


  def buildInfogreffeBusinessViews(): Unit = {
    new BuildInfogreffeBv(dataLoader).execute()
  }

  def buildPerleBusinessViews(): Unit = {
    new BuildPerleBv(dataLoader).execute()
  }

  def buildRctBusinessViews(): Unit = {
    new BuildGroupBv(dataLoader).execute()
    new BuildSubGroupBv(dataLoader).execute()
    new BuildLeBv(dataLoader).execute()
    new BuildNetworkRelationBv(dataLoader).execute()
    new BuildIdmutBv(dataLoader).execute()
    new BuildRctBv(dataLoader).execute()
  }

  def buildAvoxBusinessViews(): Unit = {
    new BuildAvoxLeBv(dataLoader).execute()
    new BuildAvoxAuditBv(dataLoader).execute()
  }

  def buildLeiBusinessViews(): Unit = {
    new BuildLeiLeBv(dataLoader).execute()
  }

  def buildDunBusinessViews(): Unit = {
    new BuildDunLeBv(dataLoader).execute()
  }

  def buildInseeBusinessViews () : Unit = {
    new BuildInseeLeBv(dataLoader).execute()
  }

  def buildGrcPmBusinessViews () : Unit = {
    new BuildGrcPmBv(dataLoader).execute()
  }

  def buildGrcPpBusinessViews () : Unit = {
    new BuildGrcPpBv(dataLoader).execute()
  }


  def buildBdrBusinessViews(): Unit = {
    new BuildBdrLeBv(dataLoader).execute()
  }

  def buildPrivaliaBusinessViews(): Unit = {
    new BuildPrivaliaPmBv(dataLoader).execute()
    new BuildPrivaliaPpBv(dataLoader).execute()
  }
}