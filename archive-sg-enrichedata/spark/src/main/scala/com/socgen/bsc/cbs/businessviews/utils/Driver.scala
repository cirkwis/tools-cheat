package com.socgen.bsc.cbs.businessviews.utils

import org.apache.spark.Partition
import org.apache.spark.sql.SQLContext
import scopt.OptionParser
import com.socgen.bsc.cbs.businessviews.common.DataFrameLoader

/**
  * Created by X149386 on 16/11/2016.
  */


case class Config(views: Seq[String] = Seq(), appName : String  = "",  partition : Option[String]  = None, database: String = "")

class ConfigParser {


  def parseArguments(config: Config, args: Array[String]): Option[Config] = {
    val parser = new OptionParser[Config]("Business Views Engine") {
      head("CBS - Business Views Engine")
      arg[String]("<view>...")
        .unbounded()
        .optional()
        .action((x, c) => c.copy(views = c.views :+ x))
        .text("Required unbounded Views args to build")
      opt[String]('p', "partitions")
        .optional()
        .valueName("<appName>")
        .action((x, c) => {
          c.copy(appName = x )
        })
        .validate( x => {
          if ( x == "bigMatching" || x == "buildThirdParties" ) success
          else failure("parameter has to be 'bigMatching' or 'buildThirdParties'")
        }
        )
        .text("partition is a an option to generate a file which contains all last partitions for each sources")
      opt[String]('s', "suffix")
        .optional()
        .valueName("<partition>")
        .action((x, c) => {
          c.copy(partition = Some(x))
        })
        .validate( x => {
          val partitionReg = "(\\d{4})/(\\d{2})/(\\d{2})".r
          x match {
            case partitionReg(yyyy, mm, dd) => success
            case _ => failure(s"parameter has to be yyyy/mm/dd. Found => ${x} ")
          }
        })
        .text("partition is a an option to generate a file which contains all last partitions for each sources")
      opt[String]('s', "hive-schema")
        .optional()
        .valueName("<database>")
        .action((x, c) => c.copy(database = x))
        .text("hive-schema is a flag to generate hive schemas for creating business views.\n" +
          "It requires as input the name of the hive database.")
      help("help")
        .text("You should provide space separated View names to build.")
    }
    parser.parse(args, config)
  }
}

class Driver() {
  var config: Config = Config()

  def readArguments(args: Array[String]): Unit = {
    val parser = new ConfigParser()
    parser.parseArguments(config, args) match {
      case Some(conf) => config = conf
      case None => System.exit(1)
    }
  }

  def loadViewBuilder(sqlContext: SQLContext): ViewBuilder = {
    val dataFrameLoader = new DataFrameLoader(sqlContext, config.partition)
    new ViewBuilder(config.views, dataFrameLoader)
  }

  def loadConfigWriter(sqlContext: SQLContext, appName : String): PartitionsWriter = {
    val dataFrameLoader = new DataFrameLoader(sqlContext)
    new PartitionsWriter(dataFrameLoader, appName)
  }

  def loadHiveSchemaCreator(sqlContext: SQLContext): HiveSchemaCreator = {
    val dataFrameLoader = new DataFrameLoader(sqlContext)
    new HiveSchemaCreator(config, dataFrameLoader)
  }
}
