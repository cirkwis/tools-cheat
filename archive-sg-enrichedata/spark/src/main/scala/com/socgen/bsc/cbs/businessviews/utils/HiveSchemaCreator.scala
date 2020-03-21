package com.socgen.bsc.cbs.businessviews.utils

import com.socgen.bsc.cbs.businessviews.avox.{BuildAvoxAuditBv, BuildAvoxLeBv}
import com.socgen.bsc.cbs.businessviews.bdr.BuildBdrLeBv
import com.socgen.bsc.cbs.businessviews.common.{BusinessView, DataFrameLoader, Frequency, MultipleViews}
import com.socgen.bsc.cbs.businessviews.dun.BuildDunLeBv
import com.socgen.bsc.cbs.businessviews.rct._
import com.socgen.bsc.cbs.businessviews.insee.BuildInseeLeBv
import com.socgen.bsc.cbs.businessviews.grc.{BuildGrcPmBv, BuildGrcPpBv}
import com.socgen.bsc.cbs.businessviews.idq.BuildIdqBv
import com.socgen.bsc.cbs.businessviews.infogreffe.BuildInfogreffeBv
import com.socgen.bsc.cbs.businessviews.lei.BuildLeiLeBv
import com.socgen.bsc.cbs.businessviews.perle.BuildPerleBv
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * Created by X149386 on 23/11/2016.
  */
class HiveSchemaCreator(config: Config, dataFrameLoader: DataFrameLoader) {
  val database = ""
  val businessViews = new ListBuffer[BusinessView]()
  val multipleViews = new ListBuffer[MultipleViews]()

  def PrintSchemaCreationStatements(): Unit = {
    val views: Seq[String] = config.views
    views.foreach(view => view match {
      case "rct" => appendRctBusinessViews()
      case "avox" => appendAvoxBusinessViews()
      case "lei" => appendLeiBusinessViews()
      case "dun" => appendDunBusinessViews()
      case "insee" => appendInseeBusinessViews()
      case "grc_pm" => appendGrcPmBusinessViews()
      case "perle_legalentity" => appendPerleBusinessViews()
      case "grc_pp" => appendGrcPpBusinessViews()
      case "infogreffe" => appendInfogreffeBusinessViews()
      case "idq" => appendIdqBusinessViews()
      case "bdr" => appendBdrBusinessViews()
      case _ =>
    })
    println(writeSchemasToString())
  }

  def appendInfogreffeBusinessViews() : Unit = {
    businessViews += new BuildInfogreffeBv(dataFrameLoader)
  }

  def appendPerleBusinessViews(): Unit = {
    businessViews += new BuildPerleBv(dataFrameLoader)
  }

  def appendRctBusinessViews(): Unit = {
    businessViews += new BuildIdmutBv(dataFrameLoader)
    businessViews += new BuildGroupBv(dataFrameLoader)
    businessViews += new BuildSubGroupBv(dataFrameLoader)
    businessViews += new BuildNetworkRelationBv(dataFrameLoader)
    businessViews += new BuildLeBv(dataFrameLoader)
  }

  def appendAvoxBusinessViews(): Unit = {
    businessViews += new BuildAvoxLeBv(dataFrameLoader)
    businessViews += new BuildAvoxAuditBv(dataFrameLoader)
  }

  def appendLeiBusinessViews(): Unit = {
    businessViews += new BuildLeiLeBv(dataFrameLoader)
  }

  def appendDunBusinessViews(): Unit = {
    businessViews += new BuildDunLeBv(dataFrameLoader)
  }

  def appendInseeBusinessViews(): Unit = {
    businessViews += new BuildInseeLeBv(dataFrameLoader)
  }

  def appendGrcPmBusinessViews(): Unit = {
    businessViews += new BuildGrcPmBv(dataFrameLoader)
  }

  def appendGrcPpBusinessViews(): Unit = {
    businessViews += new BuildGrcPpBv(dataFrameLoader)
  }
  def appendIdqBusinessViews() : Unit ={
    businessViews += new BuildIdqBv(dataFrameLoader)
  }

  def appendBdrBusinessViews(): Unit = {
    businessViews += new BuildBdrLeBv(dataFrameLoader)
  }

  def writeSchemasToString(): String = {
    val schemaBuilder: StringBuilder = new StringBuilder()

    def extractSchema (df : DataFrame , name :String, frequency: Frequency.Value ,output : String) = {
      val columns = df.schema.map(sf => sf.name + " " + sf.dataType.simpleString)
      // Print the Hive create table statement:
      schemaBuilder.append("DROP TABLE IF EXISTS " + database + "." + name + ";\n")
      schemaBuilder.append("CREATE EXTERNAL TABLE " + database + "." + name + "\n")
      schemaBuilder.append(s"  (${columns.mkString(", ")})" + "\n")
      if (frequency == Frequency.DAILY)
        schemaBuilder.append("PARTITIONED BY (yyyy STRING, mm STRING, dd String)\n")
      else if (frequency == Frequency.MONTHLY)
        schemaBuilder.append("PARTITIONED BY (yyyy STRING, mm STRING)\n")
      schemaBuilder.append("STORED AS PARQUET " + "\n")
      schemaBuilder.append("LOCATION '" + output + name + "'; \n\n")
    }

    businessViews.foreach(businessView => {
      val u = businessView.frequency
      val df = businessView.process()
      val name = businessView.name
      val output = businessView.outputDirectory
      extractSchema(df,name,u,output)
    })

    multipleViews.foreach({ multipleView =>
      val u = multipleView.frequency
      val output = multipleView.outputDirectory
      val views = multipleView.process()
      views.foreach({
        case (name,df) => {
          extractSchema(df,name,u,output)
        }
      })
    })
    schemaBuilder.toString
  }
}
