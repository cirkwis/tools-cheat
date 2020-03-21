package com.socgen.bsc.cbs.businessviews.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


/**
  * A [[org.apache.spark.sql.DataFrame]] transformation action.
  */

trait Transformation {
  def transform(df: DataFrame): DataFrame

  def targets: List[String]
}

// Some common DataFrame transformation actions

/**
  * Rename an existant [[DataFrame]] column.
  *
  * @param source the column to rename.
  * @param target the new column name.
  */
class RenameTransformation(source: String, target: String) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = df.withColumnRenamed(source, target)
}

/**
  * Copy and rename an existant [[DataFrame]] column.
  *
  * @param source the column to copy.
  * @param target the new copied column name.
  */
class CopyTransformation(source: String, target: String, keepSource: Boolean) extends Transformation {
  override def targets = keepSource match {
    case true => List(source, target)
    case false => List(target)
  }

  override def transform(df: DataFrame): DataFrame = df.withColumn(target, col(source))
}

/**
  * select a column and add it to the targets list of columns to select.
  * The transformation applied to the [[DataFrame]] column is the identity.
  *
  * @param source the column to select.
  */
class IdentityTransformation(source: String) extends Transformation {
  override def targets = List(source)

  override def transform(df: DataFrame): DataFrame = df
}

/**
  * Drop an existent [[DataFrame]] column.
  *
  * @param source the column name to drop.
  */
class DropTransformation(source: String) extends Transformation {
  override def targets = List()

  override def transform(df: DataFrame): DataFrame = df.drop(source)
}

/**
  * Create a new column which is the application of the given custom function applied to the source fields.
  *
  * @param target  the new column to create.
  * @param mrgfunc a function to apply on the fields.
  * @param fields  the fields to process.
  */
class CustomTransformation(target: String, mrgfunc: Seq[String] => String, fields: String*) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val mrgUdf = udf(mrgfunc)
    val cols = fields.map(col)
    df.withColumn(target, mrgUdf(array(cols: _*)))
  }
}


class SplitTransformation(targets_ : Seq[String], splitFunc: String => Seq[String], columnName: String) extends Transformation {
  override def targets = targets_.toList

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val splitUdf = udf(splitFunc)
    val spltInterm = df.withColumn(columnName, splitUdf(col(columnName)))
    val splittedDf = targets_.indices.foldLeft(spltInterm) { (df, idx) =>
      df.withColumn(targets_(idx), col(columnName).getItem(idx))
    }
    splittedDf.drop(columnName)
  }
}

class ExtractTransformation(target: String, extractFunc: String => String, columnToExtract: String) extends Transformation {

  override def targets: List[String] = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val extractUdf = udf(extractFunc)
    df.withColumn(target, extractUdf(col(columnToExtract)))
  }
}

class FilterTransformation(target: String,
                           filterFunc: (String, String) => String,
                           source: String,
                           filteredColumn: String) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val filterUdf = udf(filterFunc)
    df.withColumn(target, filterUdf(col(source), col(filteredColumn)))
  }
}

/**
  * class to map a column
  *
  * @param target
  * @param mapFunction
  * @param columnToMap
  */
class MapTransformation(target: String, mapFunction: Int => String, columnToMap: String) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val maperUdf = udf(mapFunction)
    df.withColumn(target, maperUdf(col(columnToMap)))
  }
}

class MapTransformation2(target: String, mapFunction: String => String, columnToMap: String) extends Transformation {
  override def targets = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val maperUdf = udf(mapFunction)
    df.withColumn(target, maperUdf(col(columnToMap)))
  }
}


/**
  * class to cast a column
  *
  * @param target  the column to cast
  * @param newType the new Type of the column
  */
class CastTransformation(target: String, newType: String) extends Transformation {
  override def targets: List[String] = List(target)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    newType match {
      case "String" => df.withColumn(target, col(target).cast(StringType))
      case "Integer" => df.withColumn(target, col(target).cast(IntegerType))
    }
  }
}

/**
  * Create a new column with a static content
  *
  * @param columnName  the new column to create.
  * @param staticValue the value of the column
  */
class CreateTransformation(columnName: String, staticValue: String) extends Transformation {
  override def targets = List(columnName)

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    df.withColumn(columnName, lit(staticValue))
  }
}

object DataFrameTransformer {
  /**
    * Rename an existent [[DataFrame]] column.
    *
    * @param source the column to rename.
    * @param target the new column name.
    */
  def renameCol(source: String, target: String) = new RenameTransformation(source, target)

  def copyCol(source: String, target: String, keepSource: Boolean = false) = new CopyTransformation(source, target, keepSource)

  def identityCol(source: String) = new IdentityTransformation(source)

  def dropCol(source: String) = new DropTransformation(source)

  /**
    * Create a new column which is the application of the given custom function applied to the source fields.
    *
    * @param target    the new column to create.
    * @param mergeFunc a function to apply on the fields.
    * @param fields    the fields to process.
    */
  def mergeFields(target: String, mergeFunc: Seq[String] => String, fields: String*) = new CustomTransformation(target, mergeFunc, fields: _*)

  def applySpecificOperation(target: String, func: (Seq[String]) => String, fields: String*) = new CustomTransformation(target, func, fields: _*)

  def splitFields(targets: Seq[String], splitFunc: String => Seq[String], field: String) = new SplitTransformation(targets, splitFunc, field)

  def applyMaper(target: String, mapFunc: Int => String, columnToMap: String) = new MapTransformation(target, mapFunc, columnToMap)

  def applyMaper(target: String, mapFunc: String => String, columnToMap: String) = new MapTransformation2(target, mapFunc, columnToMap)

  def applyFilter(target: String, func: (String, String) => String, column: String, filteredColumn: String) = new FilterTransformation(target, func, column, filteredColumn)

  def createStaticColumn(columnName: String, staticValue: String) = new CreateTransformation(columnName, staticValue)

  def castColumn(target: String, newType: String) = new CastTransformation(target, newType)

  def extract(target: String, extractFunc: String => String, columnToExtract: String) = new ExtractTransformation(target, extractFunc, columnToExtract)


  implicit class DFWithExtraOperations(df: DataFrame) {
    def applyOperations(operations: Seq[Transformation], all: Boolean = false): DataFrame = {
      val finalDF = operations.foldLeft(df)((dataframe, t) => t.transform(dataframe))
      all match {
        case false => {
          val fieldsToSelect = operations.flatMap(t => t.targets).distinct
          finalDF.select(fieldsToSelect.map(col): _*)
        }
        case true => {
          finalDF.select("*")
        }
      }
    }

    def addColumnPrefix(prefix: String): DataFrame = {
      var dataFrame: DataFrame = df
      val columns = df.columns
      columns.foreach { c =>
        dataFrame = dataFrame.withColumnRenamed(c, prefix + c)
      }
      dataFrame
    }

    def withListOfColumns(l1: List[String], l2: List[String]): DataFrame = {
      def loop(l1: List[String], l2: List[String], dataFrame: DataFrame): DataFrame = {
        (l1, l2) match {
          case (List(), List()) => dataFrame
          case (List(), head::tail) => throw new Exception("Lists don't match in size")
          case (head::tail,List()) => throw new Exception("Lists don't match in size")
          case _ => loop(l1.tail, l2.tail, dataFrame.withColumn(l1.head, when(col(l2.head).isNotNull,col(l2.head)).otherwise(col(l1.head))))
        }
      }
      if (l1.length == l2.length) loop(l1, l2, df) else throw new Exception("Lists don't match in size")
    }

    def filterDuplicates(duplicateColName: String, keysColumns: Seq[String]): DataFrame = {
      var dataFrame: DataFrame = df

      import org.apache.spark.sql.expressions.Window

      dataFrame.withColumn(duplicateColName , count("*").over(Window.partitionBy(keysColumns.map{col(_)} : _*)))
        .dropDuplicates(keysColumns)
    }


  }

}

