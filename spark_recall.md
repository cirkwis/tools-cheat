*Ways of Defining Schema*

|Construct schema using Case Class|Construct schema programmatically |
|---|---|
|Use to construct Datasets when columns and types are known at runtime|Use to construct Datasets or DataFrames when columns and types are not known until runtime|
| Scala Case Class restriction to 22 fields |Use if schema has more than 22 fields| 

*Datasets vs. DataFrames Creating*

![alt text](./spark-1.png "Datasets vs. DataFrames")

![alt text](./spark-2.png "Datasets vs. DataFrames")

*Create a Dataset*

```java
import spark.implicits._

case class Incidents(incidentnum:String, category: String, description: String, dayofweek: String, date: String, time: String, ppdistrict: String, resolution: String, address: String, X: Double, Y: Double, pdid:String)

val sfpdDS = spark.read.csv("path to file").as[Incidents]

sfpdDS.createTempView("sfpd")
```
*Details: Load Data*

* spark.read.load(“path/filename.parquet”)
  
  * Data type: Parquet (default)
  * Loads data in path. Default data type is parquet. For other formats, use the load(path).format method. Default data type can be configured using spark.sql.sources.default property.

* spark.read.load(path).format(type)
  
  * Data type: JSON, Parquet, CSV
  * Loads data in path with type specified in the format method.

* spark.read.text
  
  * Data type: Text File
  * Loads a text file and returns Dataset[Row]

* spark.read.textfile
  
  * Data type: Text File
  * Loads a text file and returns Dataset[String]. This method can be used when you want the return type to be Dataset[String] instead of DataFrame (i.e. Dataset[Row])

* spark.read.jdbc(URL, Table, Connection_Properties)
  * Data type: Database Table
  * Returns a DataFrame with data from a database table. Used to load data directly from a database table using a JDBC connection.

* spark.read.csv(path_to_CSV_file)
  * Data type: CSV
  * Loads CSV data in path. Similar to spark.read.load(path).format("csv")

* spark.read.json(path_to_JSON_file)
  * Data type: JSON
  * Loads JSON data in path. Similar to spark.read.load(path).format("json")

* spark.read.parquet(path_to_parquet_file)
  * Data type: Parquet
  * Loads parquet data in path. Similar to spark.read.load(path).format("parquet")

* Default data source can be configured here: spark.sql.sources.default

*Creating a Dataframe and Construct Schema Programmatically*

```java
import org.apache.spark.sql.types._

// Create an RDD
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

// The schema is encoded in a string
val schemaString = "name;age"

// Generate the schema based on the string of schema
val fields = schemaString.split(";")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)
/*
val schema = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("age", StringType, nullable = true)))
*/

// Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL can be run over a temporary view created using DataFrames
val results = spark.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
results.map(attributes => "Name: " + attributes(0)).show()
// +-------------+
// |        value|
// +-------------+
// |Name: Michael|
// |   Name: Andy|
// | Name: Justin|
// +-------------+
```

*Convert DataFrames to Datasets*

![alt text](./spark-3.png "Convert DataFrames to Datasets")

*Convert DataFrames to Datasets with nested document*
```java
case class Metric(m1: String, m2: String, m3: String)

case class Dimension(d1: String, d2: String, d3: String, metric: Metric)

import org.apache.spark.sql.functions.struct
import spark.implicits._

...

df.select($"d1", $"d2", $"d3", struct($"m1", $"m2", $"m3").alias("metric")).as[Dimension]
```

*User Defined Functions*
  
* Two types of UDFs:
  * used with scala (dataset operations)
  * used with sql

* Defining UDF: 
 ```python 
  val udfDefined = udf((arguments) => {function definition})
```

![alt text](./spark-4.png "UDF: Scala")

*UDF: SQL*

 ```python 
  spark.udf.register("function name",function definition)
```
![alt text](./spark-5.png "UDF: SQL")