package com.socgen.bsc.cbs.businessviews.base

import com.socgen.bsc.cbs.businessviews.common.CommonUdf._
import com.socgen.bsc.cbs.businessviews.common.{DataFrameLoader, LoggingSupport}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * This class contains multiple functions to prepare link before they are inserted in the egine
  * Created by X156170 on 23/01/2018.
  */


object InterceptorFunctions extends LoggingSupport {

  /**
    * This class represents a shareHoleder by his name and his pct,
    * it's extends Ordered to implements a way to compare them
    *
    * @param name is obviously the shareholder's name
    * @param pct  is not obviously the amount of precentage the shareholder has
    */
  case class Shareholder(name: String, pct: Int) extends Ordered[Shareholder] {
    override def compare(that: Shareholder): Int = that.pct - this.pct
  }

  /**
    * This class represents one thrid_parties' status
    *
    * @param name        is the name of the status
    * @param last_update is the last time the status has been updated
    */
  case class Status(name: String, last_update: String) extends Ordered[Status] {
    override def compare(that: Status): Int = that.name compareTo this.name
  }

  /**
    * This function gives a safe access to every elements in a list
    *
    * @param l     is the list we want to have access elements of
    * @param index is the element's index we want in the list
    * @tparam T represents the class which the list is composed of
    * @return an Either object, right[T] if the index exists in the list,
    *         else a Left[String] with neutral message in it
    */
  def safeGet[T](l: List[T], index: Int): Either[String, T] = {
    if (index > l.length - 1) Left("None") else Right(l(index))
  }


  //TODO : Find a way to make the function extract generic for every type ? pros : no duplicated code, cons :code less readable
  /**
    * extract an shareHolder object which is encapsuled in an Either Object
    *
    * @param either
    * @return the shareHolder in right if exists else return a shareholder with neutral values
    */
  def extractShareHolder(either: Either[String, Shareholder]) = {
    either match {
      case x: Left[String, Shareholder] => Shareholder("None", 0)
      case x: Right[String, Shareholder] => Shareholder(x.right.get.name, x.right.get.pct)
    }
  }

  /**
    * extract a Status object which is encapsuled in an Either object
    *
    * @param either
    * @return the Status in right if exists else return a Status with neutral values
    */
  def extractStatus(either: Either[String, Status]) = {
    either match {
      case x: Left[String, Status] => Status("None", "None")
      case x: Right[String, Status] => Status(x.right.get.name, x.right.get.last_update)
    }
  }


  /**
    * This function is specific to the file HADT37
    * It transforms it by grouping the shareholders by id and making a ranking order by highest percentage
    *
    * @param sQLContext to use some functions in implicits package
    * @param df         The dataframe to transform
    * @return a new dataframe
    */
  def prepareHADT37(sQLContext: SQLContext)(df: DataFrame): DataFrame = {
    import sQLContext.implicits._
    val columnsShareHolders = Seq("NUM_PM", "X_NOM_ACTI", "X_PCT_ACTI")
    val columnsState = Seq("NUM_PM", "ETAT")
    val finalColumns = Array("NUM_PM", "X_NOM_ACTI_1", "X_PCT_ACTI_1", "X_NOM_ACTI_2",
      "X_PCT_ACTI_2", "X_NOM_ACTI_3", "X_PCT_ACTI_3")

    val shareHoldersDf = df.select(columnsShareHolders.map(c => col(c)): _*)
    val shareHoldersRdd = shareHoldersDf.rdd
    val finalShareHoldersRdd = shareHoldersRdd.map(r => (r(0).asInstanceOf[String], new Shareholder(r(1).asInstanceOf[String], r(2).asInstanceOf[String].toInt)))
      .groupByKey()
      .map { case (id, shareholders) => (id, shareholders.toList.sorted) }
      .map { case (id, shareholders) => (id, safeGet(shareholders, 0), safeGet(shareholders, 1), safeGet(shareholders, 2)) }
      .map { case (id, sh1, sh2, sh3) => (id, extractShareHolder(sh1), extractShareHolder(sh2), extractShareHolder(sh3)) } //sh for shareHolder
      .map { case (id, sh1, sh2, sh3) => (id, sh1.name, sh1.pct.toString, sh2.name, sh2.pct.toString, sh3.name, sh3.pct.toString) }
    val finalShareHoldersDf = finalShareHoldersRdd.toDF(finalColumns: _*)

    val malformedDf = finalShareHoldersDf.filter("X_NOM_ACTI_1 = 'None' or X_NOM_ACTI_2 = 'None' or X_NOM_ACTI_3 = 'None'")
    if (malformedDf.count() != 0) {
      dataFramelogging(malformedDf, "These third_parties have no expected values, they are replaced by 'None'")
    }

    val stateDf = df.select(columnsState.map(c => col(c)): _*).rdd
      .map(r => (r(0).asInstanceOf[String], r(1).asInstanceOf[String]))
      .groupByKey()
      .map(l => (l._1, l._2.head))
      .toDF(columnsState: _*)

    finalShareHoldersDf.join(stateDf, "NUM_PM")

  }

  /**
    * This function prepare a link by applying the same proccess than the whole engine process
    *
    * @param name The name of the source we want to prepare
    * @param dataLoader
    * @return a new dataframe which is update according to a file cong
    *         example : HADT08 and HADT09 are prepared by this function
    */
  def prepare(name: String)(dataLoader: DataFrameLoader): DataFrame = {
    val configData = ConfigFactory.load("bv_engine/" + name + ".conf")
    val settings = new Settings(configData, name, dataLoader)
    val setUp = StockUpdate.loadSettings(settings)
    StockUpdate.compute(setUp, None)
  }

  /**
    * This function transform the link by grouping them by id and create new fields on the DF
    * This is a very specific function to some files in delta procesing
    *
    * @param sQLContext
    * @param df
    * @param id
    * @return
    */
  def prepareMonacoBddf(sQLContext: SQLContext)(df: DataFrame)(id: String): DataFrame = {
    import sQLContext.implicits._

    val columnsState = Seq(id, "ETAT")
    val columns = Seq(id, "X_CRS_STATUT", "X_DT_LST_CRS_FA")
    val finalColumns = Array(id, "X_CRS_STATUT_MONACO", "X_DT_LST_CRS_FA_MONACO",
      "X_CRS_STATUT_BDDF", "X_DT_LST_CRS_FA_BDDF")

    val reducedDf = df.select(columns.map(c => col(c)): _*)
    val rdd = reducedDf.rdd
    val computedDf = rdd.map(r => (r(0).asInstanceOf[String], (new Status(r(1).asInstanceOf[String], r(2).asInstanceOf[String]))))
      .groupByKey()
      .map { case (id, status) => (id, status.toList.sorted) }
      .map { case (id, status) => (id, safeGet(status, 0), safeGet(status, 1)) }
      .map { case (id, s1, s2) => (id, extractStatus(s1), extractStatus(s2)) }
      .map { case (id, s1, s2) => (id, s1.name, s1.last_update, s2.name, s2.last_update) }
      .toDF(finalColumns: _*)

    val malformedDf = computedDf.filter("X_CRS_STATUT_MONACO = 'None' or X_CRS_STATUT_BDDF = 'None'")
    if (malformedDf.count() != 0) {
      dataFramelogging(malformedDf, "These third_parties have no expected values, they are replaced by 'None'")
    }

    val stateDf = df.select(columnsState.map(c => col(c)): _*).rdd
      .map(r => (r(0).asInstanceOf[String], r(1).asInstanceOf[String]))
      .groupByKey()
      .map(l => (l._1, l._2.head))
      .toDF(columnsState: _*)
    computedDf.join(stateDf, id)
  }

  /**
    * Generic function which is applied to multiple delta sources, it computes a sign and extracte a substring
    * based on an index
    *
    * @param mapCols This map contains the existing column as a key and the column we want as result as value
    * @param index   is the number of character we want to substract from the input
    * @return
    */
  def prepareMapping(mapCols: Map[String, Option[String]])
                    (index: Int) = (dataFrame: DataFrame) => {

    val computeSignUdf = udf(computeSign())
    val extractValueAndConcatenateUdf = udf(extractValueAndConcatenate(index))
    val extractValueAndConcatenateWithKeyNotFoundUdf = udf(extractValueAndConcatenateWithKeyNotFound(index))

    def computeDf(keyValue: (String, Option[String]))(dataFrame: DataFrame) = {
      val k = keyValue._1
      val v = keyValue._2
      v match {
        case Some(v) => {
          comuteDfSubTask(dataFrame, v, k)
        }
        case None => {
          comuteDfSubTask(dataFrame, k, k)
        }
      }
    }

    /**
      * Generic function which process generic algorithm.
      *
      * @param dataFrame : DataFrame contain main data to process.
      * @param colName   String : Column that will be override
      * @param colUdf    String : Column tha will be process by UDF
      * @return
      */
    def comuteDfSubTask(dataFrame: DataFrame, colName: String, colUdf: String): DataFrame = {
      val res = dataFrame.withColumn("SIGNE_" + colName, computeSignUdf(col(colUdf)))
      val dfWithkeyNotFound = res.withColumn(colName + "_BIS", extractValueAndConcatenateWithKeyNotFoundUdf(col(colUdf)))
        .filter(colName + "_BIS = '" + keyNotFound + "'")

      if (dfWithkeyNotFound.count() > 0) {
        dataFramelogging(dfWithkeyNotFound, "[extractValueAndConcatenate()] Key mapping not match on thoses rows!")
      }

      res.withColumn(colName, extractValueAndConcatenateUdf(col(colUdf)))
    }

    def loop(keyValues: List[(String, Option[String])], dataFrame: DataFrame): DataFrame = {
      keyValues match {
        case List() => dataFrame
        case head :: tail => loop(tail, computeDf(head)(dataFrame))
      }
    }

    loop(mapCols.toList, dataFrame)
  }

  /**
    * Generic function which log dataframe content in warning on log file.
    *
    * @param df  DataFrame :  contain values to log.
    * @param msg String : Is the msg
    * @return Unit
    */
  def dataFramelogging(df: DataFrame, msg: String): Unit = {
    val header = df.schema.map(f => f.name).mkString(",")
    df.foreachPartition(x => {
      if (x.nonEmpty) {
        warning(msg + ":\n" + header + "\n")
        x.foreach(r => warning(r.toString()))
      }
    })
  }
}
