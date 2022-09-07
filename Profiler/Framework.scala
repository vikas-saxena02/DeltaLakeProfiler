// Databricks notebook source
// MAGIC %sql
// MAGIC create widget text databaseNames default 'DFLBIE,DPYAIS,DPYAML,DPYATM,DPYBCG,DPYBCM,DPYBIT,DPYBKL,DPYCB,DPYCCP,DPYCDMS,DPYCMS,DPYCSM,DPYCVA,DPYD5,DPYEDW,DPYFAT,DPYGNL,DPYIPS,DPYIRB,DPYIRS,DPYKLM,DPYLCR,DPYLOS,DPYMCS,DPYML,DPYOBR,DPYOLS,DPYPBFE,DPYPPS,DPYRBF,DPYRPA,DPYSBG,DPYSCBC,DPYSCR,DPYSFC,DPYST,DPYTRD,DPYTVA,DPYUTM,DPYWBG,DPYWINP,DSGCFE,DSGEDW,DSGPPS,DTPAML,DTPATM,DTPB2K,DTPBCM,DTPBIT,DTPBKL,DTPCB,DTPCDMS,DTPCMS,DTPCSENT,DTPCSM,DTPD5,DTPDMS,DTPEDW,DTPEFS,DTPESN,DTPFES,DTPGNL,DTPIRB,DTPIRS,DTPKLM,DTPLCS,DTPLEAD,DTPLOS,DTPMCS,DTPML,DTPOLS,DTPPBFE,DTPPDPA,DTPPPS,DTPRPA,DTPSBG,DTPSCR,DTPSFC,DTPTRD,DTPUTM,DTTBO,DTTCFE,DTTEDW,DTTESL,DTTLCR,DTTPBFE,DTTPPS,DUTEDW'

// COMMAND ----------

// DBTITLE 1,1.1 Utils Functions
// MAGIC %run ./Functions

// COMMAND ----------

// DBTITLE 1,2. Fetch Database names
var databaseNames = dbutils.widgets.get("databaseNames")
var dbNames = if(databaseNames.trim.size ==0 ) spark.sql("show databases").collect.map(row => row.getString(0)).toList else databaseNames.split(",").map(db => "P1" + db).toList

// COMMAND ----------

// DBTITLE 1,3. Fetch all tables and Views
//DFLBIE,DPYAIS,DPYAML,DPYATM,DPYBCG,DPYBCM,DPYBIT,DPYBKL,DPYCB,DPYCCP,DPYCDMS,DPYCMS,DPYCSM,DPYCVA,DPYD5,DPYEDW,DPYFAT,DPYGNL,DPYIPS,DPYIRB,DPYIRS,DPYKLM,DPYLCR,DPYLOS,DPYMCS,DPYML,DPYOBR,DPYOLS,DPYPBFE,DPYPPS,DPYRBF,DPYRPA,DPYSBG,DPYSCBC,DPYSCR,DPYSFC,DPYST,DPYTRD,DPYTVA,DPYUTM,DPYWBG,DPYWINP,DSGCFE,DSGEDW,DSGPPS,DTPAML,DTPATM,DTPB2K,DTPBCM,DTPBIT,DTPBKL,DTPCB,DTPCDMS,DTPCMS,DTPCSENT,DTPCSM,DTPD5,DTPDMS,DTPEDW,DTPEFS,DTPESN,DTPFES,DTPGNL,DTPIRB,DTPIRS,DTPKLM,DTPLCS,DTPLEAD,DTPLOS,DTPMCS,DTPML,DTPOLS,DTPPBFE,DTPPDPA,DTPPPS,DTPRPA,DTPSBG,DTPSCR,DTPSFC,DTPTRD,DTPUTM,DTTBO,DTTCFE,DTTEDW,DTTESL,DTTLCR,DTTPBFE,DTTPPS,DUTEDW
import com.databricks.sql.transaction.tahoe._
import org.apache.spark.sql.catalyst._
import com.databricks.sql.transaction.tahoe.commands._
import org.apache.spark.sql.catalyst.catalog._

val parallismHandler = new ParallellismHandler[String]
var allTables = parallismHandler.withParallelism(dbNames, 10).map(
  dbName => {
    var error = false
    var listOfTables = List((dbName,false,"Incorrect DB Name"))
    try {
      var data = spark.sql("show tables in "+ dbName).collect()
      data.map(row => (dbName,true,row.getString(1))).toList
    } catch {
      case _: Throwable =>   {
        error = true
        listOfTables
      }
    }
  }
).flatten.toList

// COMMAND ----------

// DBTITLE 1,3.1 Count of Tables and Views ( Debug Command)
allTables.filter(record => record._2).size

// COMMAND ----------

// DBTITLE 1,4. Filter out Tables
// MAGIC %scala
// MAGIC val parallismHandler = new ParallellismHandler[(String,Boolean,String)]
// MAGIC val justTables = parallismHandler.withParallelism(allTables.filter(record => record._2), 10)
// MAGIC   .map( record => spark.sessionState.catalog.getTableMetadata(TableIdentifier(record._3,Option.apply(record._1.toString))))
// MAGIC   .filter(catalog => catalog.tableType != CatalogTableType.VIEW).toList

// COMMAND ----------

// DBTITLE 1,5. Fetch Table Details

import org.apache.spark.sql.catalyst.catalog.CatalogTable
val parallismHandler = new ParallellismHandler[CatalogTable]
val tableDetails = justTables.par.map(catalog => fetchTableDetails(spark,Option.apply(catalog)))

// COMMAND ----------

// DBTITLE 1,5. 1 Convert Table Details to DataFrame
import spark.implicits._
val tableDetailsDF = tableDetails.toList.toDF

// COMMAND ----------

// DBTITLE 1,5. 2 Format Dataframe to required schema
import org.apache.spark.sql.functions._
val tbLDetails = tableDetailsDF.select(
  col("name").as("table"),
  col("format").as("tableFormat"),
  expr("size(partitionColumns)>0").as("isPartitioned"),
  col("sizeInBytes").as("tableSizeInBytes"),
  col("numFiles").as("numFiles"),
  col("partitionCount").as("numberOfPartitions"),
  expr("case when numFiles > 0 then sizeInBytes/numFiles else 0 end").as("avgFileSizeInBytes"),
  expr("case when partitionCount > 0 then sizeInBytes/partitionCount else 0 end").as("avgPartitionSizeInBytes"),
  expr("length(trim(nvl(zOrderColumns,''))) > 0").as("isZordered"),
  col("zOrderColumns").as("zOrderDetails"),
)

// COMMAND ----------

// DBTITLE 1,5. 3 Format Dataframe to required schema
val finalOutput = tbLDetails.withColumn("tableSizeInMB", $"tableSizeInBytes"/(1024*1024)).withColumn("avgFileSizeInMB", $"avgFileSizeInBytes"/(1024*1024)).withColumn("avgPartitionSizeInMB", $"avgPartitionSizeInBytes"/(1024*1024))

// COMMAND ----------

// DBTITLE 1,5. 4 Debug final Dataframe
finalOutput.createOrReplaceTempView("finalOutput")
display(finalOutput)

// COMMAND ----------

// DBTITLE 1,6 Write stats captured into Delta table
// MAGIC %sql
// MAGIC create or replace table physicalTableStats as select * from finalOutput

// COMMAND ----------

// DBTITLE 1,6.1 Debug data written in table
// MAGIC %sql
// MAGIC select * from physicalTableStats where  tablesizeinMB >100 and avgFileSizeInMB <15
