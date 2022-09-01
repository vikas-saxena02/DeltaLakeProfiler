// Databricks notebook source
// dbutils.widgets.text("databaseName", "")

// COMMAND ----------

// MAGIC %run
// MAGIC ./Functions

// COMMAND ----------

val dbName = dbutils.widgets.get("databaseName").trim()

// COMMAND ----------

if(dbName.equals("")) {
  dbutils.notebook.exit("-1")
}

// COMMAND ----------

import org.apache.spark.sql.functions._
val tables = spark.sql("show tables in " + dbName).filter(!$"tableName".like("%view%")).withColumn("fullTableName", concat(col("database"), lit("."), col("tableName"))).drop("database").drop("tableName").drop("isTemporary").map(f=>f.getString(0)).collect.toList

// COMMAND ----------

println(tables)

// COMMAND ----------

var results = List[Result]()
for (table <- tables) {
  val row:Row = getTableDetails(table)
  val tableFormat = row.getAs[String](0)
  val partitionColumns = row.getAs[Seq[String]](7)
  val numFiles = row.getAs[Long](8)
  val tableSizeInBytes = row.getAs[Long](9)
  
  var isDelta = false
  if(tableFormat.equalsIgnoreCase("delta")){
    isDelta = true
  }
  
  var isZordered: Boolean = false
  var zOrderDetails: Seq[String] = Seq[String]()
  if(isDelta){
    zOrderDetails = getZOrderedDetails(table)
    if(zOrderDetails.size != 0) {
      isZordered = true
    }
  }
  
  
  val isPartitioned = isTablePartitioned(partitionColumns)
  
  var avgFileSizeInBytes = 0.0
  if(numFiles !=0) {
    avgFileSizeInBytes = tableSizeInBytes/numFiles
  }
  
  var numberOfPartitions: Long  = 0
  var avgPartitionSizeInBytes = 0.0
  if(isPartitioned) {
    numberOfPartitions = getPartitionCount(table)
    if(numberOfPartitions != 0) {
      avgPartitionSizeInBytes = tableSizeInBytes/numberOfPartitions
    }
  }
 
 
 results = Result(table, tableFormat, isPartitioned, partitionColumns, tableSizeInBytes, numFiles, numberOfPartitions, avgFileSizeInBytes, avgPartitionSizeInBytes, isZordered, zOrderDetails) :: results 
}

// COMMAND ----------

val output = results.toSeq.toDF()

// COMMAND ----------

output.show(100, false)

// COMMAND ----------

output.count

// COMMAND ----------

output.groupBy("isPartitioned").count().show(false)

// COMMAND ----------

output.groupBy("isZOrdered").count().show(false)

// COMMAND ----------

val finalOutput = output.withColumn("tableSizeInMB", $"tableSizeInBytes"/(1024*1024)).withColumn("avgFileSizeInMB", $"avgFileSizeInBytes"/(1024*1024)).withColumn("avgPartitionSizeInMB", $"avgPartitionSizeInBytes"/(1024*1024))

// COMMAND ----------

finalOutput.show(100,false)

// COMMAND ----------

display(finalOutput)

// COMMAND ----------

display(finalOutput.filter($"isPartitioned" === true).filter($"avgPartitionSizeInMB" > 1000))

// COMMAND ----------

display(finalOutput.filter($"isPartitioned" === true).filter($"avgPartitionSizeInMB" < 1000))
