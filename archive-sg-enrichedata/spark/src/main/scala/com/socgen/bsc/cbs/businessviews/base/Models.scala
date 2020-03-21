package com.socgen.bsc.cbs.businessviews.base

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._

/**
  * Created by X153279 on 11/01/2018.
  */

//TODO : add comments later
class Base(baseDf: DataFrame, val name: String, pk: List[String]) {

  val STATE = "ETAT"

  def compute(links: List[Link],
              interceptors: Option[List[Interceptor]],
              columns: List[Column]): DataFrame = {

    val enrichedBase = interceptors match {
      case Some(interceptors) => addLinks(links, Some(interceptors))
      case None => addLinks(links, None)
    }

    val enrichedBaseWithColumns = Base.addColumns(enrichedBase, columns)

    // select only the calculated columns
    val selectedDf = Base.selectOnlyCalculatedColumns(enrichedBaseWithColumns, columns)

    // rename the computed prefixed column
    Base.renameComputedColumn(selectedDf, columns)
  }

  def addLinks(links: List[Link],
               interceptors: Option[List[Interceptor]]): DataFrame = {
    interceptors match {
      case Some(interceptors) => {
        val interceptorsMap = interceptors.map(i => (i.dataSetName -> i.enrichedDataSet)).toMap
        links.foldLeft(baseDf) {
          (df, link) => {
            val linkTransformed = interceptorsMap.get(link.name) match {
              case Some(enrichedDf) => Base.prefixLink(Link(enrichedDf, link.name, link.pk))
              case None => Base.prefixLink(link)
            }
            val linkDf = linkTransformed.df.get
            df.join(linkDf, Base.evaluateJoinCondition(df, linkDf)(pk, linkTransformed.pk), "full_outer")
              .filter((col(link.name + "_" + STATE) !== "S") || (col(link.name + "_" + STATE).isNull))
              .withListOfColumns(pk, linkTransformed.pk)

          }
        }
      }
      case None => {
        links.foldLeft(baseDf) {
          (df, link) => {
            val prefixedLink = Base.prefixLink(link)
            df.join(prefixedLink.df.get, Base.evaluateJoinCondition(df, prefixedLink.df.get)(pk, prefixedLink.pk), "full_outer")
              .filter((col(link.name + "_" + STATE) !== "S") || (col(link.name + "_" + STATE).isNull))
              .withListOfColumns(pk, prefixedLink.pk)
          }
        }
      }
    }
  }

}

object Base {

  import org.apache.spark.sql.{Column => Col}

  def asColOrAsString(value: String): sql.Column = value match {
    case value if value startsWith ("$") => col(value.tail)
    case _ => lit(value)
  }

  def evaluateJoinCondition(df1: DataFrame, df2: DataFrame)(pk1: List[String], pk2: List[String]): Col = {
    if (pk1.length == pk2.length) {
      val mapKeys = pk1.zip(pk2)
      mapKeys.foldLeft(lit(true))((column, t) => column && (df1.col(t._1) === df2.col(t._2)))
    }
    else {
      throw new Exception("[Config] : Lists don't match in size")
    }
  }

  def addColumns(df: DataFrame, cols: List[Column]): DataFrame = {
    cols.foldLeft(df) {
      (df, column) => {
        df.withColumn(column.prefixedName, column.compute())
      }
    }
  }

  def selectOnlyCalculatedColumns(df: DataFrame, columns: List[Column]): DataFrame =
    df.select(columns.map { c => col(c.prefixedName) }: _*)

  def renameComputedColumn(df: DataFrame, columns: List[Column]): DataFrame = {
    columns.foldLeft(df) {
      (df, column) => {
        df.withColumnRenamed(column.prefixedName, column.name)
      }
    }
  }

  /**
    * adding prefix to the Link
    *
    * @return
    */
  def prefixLink(link: Link): Link = {
    val prefixedKey = link.pk.map(columnName => s"${link.name}_${columnName}")
    val prefixedDf = link.df.get.columns.foldLeft(link.df.get) {
      (df, columnName) => {
        df.withColumnRenamed(columnName, s"${link.name}_${columnName}")
      }
    }
    Link(Some(prefixedDf), link.name, prefixedKey)
  }

}

case class Link(df: Option[DataFrame], name: String, pk: List[String])
case class Column(name: String,
                  potentialValues: Option[List[PotentialValue]],
                  mappingWithSingleColumn: Option[String],
                  defaultValue: Option[String]) {

  val prefixedName = s"_$name"

  def compute(): sql.Column = {

    //TODO REMOVE THIS ONE BUT NOT BLOCKING
    (potentialValues, mappingWithSingleColumn, defaultValue) match {
      case (None, Some(y), None) => when(Base.asColOrAsString(y) !== "NULL", Base.asColOrAsString(y)).otherwise(col(name))
      case (None, Some(y), Some(z)) => when(Base.asColOrAsString(y) !== "NULL", Base.asColOrAsString(y)).otherwise(lit(z))
      case (None, None, None) => col(name)
      case (Some(x), None, None) => aggregateAllConditions()
      case (Some(x), Some(y), Some(z)) => throw new Exception(s"[Config] The column ${name} can't be configured this way")
      case (Some(x), Some(y), None) => throw new Exception(s"[Config] The column ${name} can't be configured this way")
      case (Some(x), None, Some(z)) => aggregateAllConditions()
      case (None, None, Some(z)) => lit(z)
    }
  }

  def aggregateAllConditions(): sql.Column = {
    def loop(l: List[PotentialValue], accumulator: Condition): Condition = l match {
      case List() => accumulator
      case y :: ys => loop(ys, accumulator.aggregate(y.condition))
    }
    loop(potentialValues.get.tail, potentialValues.get.head.condition).eval().otherwise(col(name))
  }
}

case class PotentialValue(val value: String,
                          val condition: Condition)

trait Condition {
  def eval(): sql.Column

  def aggregate(condition: Condition): Condition = condition match {
    case sl: SqlLike => AggregatedCondition(eval().
        when(sl.colCondition && (Base.asColOrAsString(sl.columnOtherwise) !== "NULL")  && (Base.asColOrAsString(sl.columnOtherwise) !== "null")
          , Base.asColOrAsString(sl.columnOtherwise)))
    case _ => throw new Exception("Not treated yet")
  }
}

case class AggregatedCondition(condition: sql.Column) extends Condition {
  def eval(): sql.Column = {
    condition
  }
}

case class SqlLike(sqlInstruction: String, columnOtherwise: String) extends Condition {
  val sqlInstructionSplited = sqlInstruction.split(" ")
  val firstColumnOrValue = sqlInstructionSplited(0)
  val operator = sqlInstructionSplited(1)
  val secondColumnOrValue = sqlInstructionSplited(2)

  def colCondition = operator match {
    case "==" => Base.asColOrAsString(firstColumnOrValue) === Base.asColOrAsString(secondColumnOrValue)
    case "!=" => Base.asColOrAsString(firstColumnOrValue) !== Base.asColOrAsString(secondColumnOrValue)
  }

  // If the expected value [columnOtherwise column value] is NULL, There is no need to compute the condition.
  def eval(): sql.Column = {
    when(colCondition && (Base.asColOrAsString(columnOtherwise) !== "NULL") && (Base.asColOrAsString(columnOtherwise) !== "null"),
      Base.asColOrAsString(columnOtherwise))
  }
}
