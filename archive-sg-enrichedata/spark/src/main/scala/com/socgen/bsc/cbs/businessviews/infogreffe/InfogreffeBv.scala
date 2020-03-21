package com.socgen.bsc.cbs.businessviews.infogreffe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import com.socgen.bsc.cbs.businessviews.common.{DataFrameLoader, Transformation}
import com.socgen.bsc.cbs.businessviews.common.DataFrameTransformer._
import com.socgen.bsc.cbs.businessviews.infogreffe.InfogreffeBvTools._

import scala.collection.mutable
import scala.util.Try

/**
  * Created by Guy Stephane Manganneau on 11/01/2017.
  */
trait InfogreffeBv {

  /**
    * Take a iterable of dataFrame and return the union of all of them
    *
    * @param dataFrames List of DataFrame
    * @return
    */
  def unionData(dataFrames: Iterable[DataFrame]): DataFrame = {
    if (dataFrames.tail.isEmpty) dataFrames.head
    else {
      dataFrames.head.unionAll(unionData(dataFrames.tail))
    }
  }

  /**
    * @param rdd        the one who want to transform to DataFrame
    * @param structure  the structure of the DataFrame we want to have
    * @param sQLContext the sqlContext for the creation of the DataFrame
    * @return convert RDD to DataFrame with the specific structure
    */
  def rddToDf(rdd: RDD[Row], structure: Array[StructField], sQLContext: SQLContext): DataFrame = {
    sQLContext.createDataFrame(rdd, new StructType(structure))//TODO : fix
  }

  /**
    * @param dataFrames List of all dataFrames from different years
    * @return a DataFrame which in, all the columns are updated with the most updated value not null
    */
  def assemblyFinalDataFrame(dataFrames: Iterable[DataFrame]): DataFrame = {
    val unionDataRdd = unionData(dataFrames).rdd
    val finalRdd = unionDataRdd
      .map(row => mapElementsOfListWithTheLast(row))
      .reduceByKey((r1, r2) => getTheMostUpdateFields(r1, r2))
      .mapValues(l => for (couple <- l) yield couple._1)
      .map(t => t._1 :: t._2)
      .map(l => Row.fromSeq(l))
    rddToDf(finalRdd, getFinalStructFields().toArray, getDataLoader().sqlContext)
  }

  /**
    * @param name      Name of the field
    * @param fieldType Type of field
    * @return a structField object with this name and this Datatype
    */
  def structField(name: String, fieldType: DataType): StructField = new StructField(name, fieldType) //TODO : Rename

  /**
    *
    * @param df   DataFrame we want to Transform
    * @param year This parameter allow to use the corresponding transformations with this year
    * @return A transformed DataFrame based on the list of transformations for this specific year
    */
  def applyProcessForYear(df: DataFrame, year: String): DataFrame = {
    df.applyOperations(getTransformationsForAYear(year))
  }

  /**
    * @param year
    * @return the list of tranformations  to apply to data which corresponds to this year
    */
  def getTransformationsForAYear(year: String): List[Transformation] = {
    (for {(k, v) <- getTransformationsAndYears(); if v.contains(year) || v.contains("all")} yield k).toList
  }

  /**
    *
    * @param df     DataFrame we want to check if it has this column
    * @param column The name of the column we want to check
    * @return True if the column exists and False if not
    */
  def hasColumn(df: DataFrame, column: String): Boolean = Try(df(column)).isSuccess

  /**
    *
    * @param df The dataFrame we want to normalize
    * @return A normalized dataFrame which all the specific columns
    *         we need for the process (with "null" values if it's not
    *         in the original DataFrame)
    */
  def normalizeDataFrame(df: DataFrame): DataFrame = {
    val columnsMapToFields = (for {(k, v) <- getTmpOutputStructure()} yield (col(k), k)).toList
    val columnUnzipFields = columnsMapToFields.unzip
    val columns = columnUnzipFields._1
    val fields = columnUnzipFields._2
    fields.foldLeft(df)((dataframe, f) => if (!hasColumn(df, f)) dataframe.withColumn(f, lit(null)) else dataframe).select(columns: _*)
  }

  /**
    * @return the List of all transformed and normalized DataFrames
    */
  def getTransformedAndNormalizedDataFrames(): List[DataFrame] = {
    (for ((k, v) <- getDataFramesAndYears()) yield {
      normalizeDataFrame(applyProcessForYear(getDataFrames()(k), v))
    }).toList
  }

  /**
    * @return the final structure of the DataFrame
    */
  def getFinalStructFields(): Iterable[StructField] = {
    for ((k, v) <- getTmpOutputStructure() - "year") yield structField(k, v)
  }

  /**
    * @return return a Map of DataFrames indexes
    *         (in the the list of dataFrames) and Year
    */
  def getDataFramesAndYears(): Map[Int, String]

  /**
    * @return a list of dataFrame based on the list of names
    */
  def getDataFrames(): List[DataFrame] = {
    for (name <- getDataFramesNames()) yield getDataLoader().load(name).withColumn("year", lit(name.split("_").last.toInt))
  }

  /**
    * @return a map of transformations and a list of the years we have to
    *         apply these
    */
  def getTransformationsAndYears(): mutable.LinkedHashMap[Transformation, List[String]]

  /**
    * @return a temporary structure to make the processing on
    */
  def getTmpOutputStructure(): mutable.LinkedHashMap[String, DataType]


  /**
    * @return the list of dataframes names
    */
  def getDataFramesNames(): List[String]


  /**
    *
    * @return the DataLoader
    */
  def getDataLoader(): DataFrameLoader

}
