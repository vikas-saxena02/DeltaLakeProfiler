// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

case class Result(table:String, tableFormat:String, isPartitioned: Boolean, paritionColumns: Seq[String], tableSizeInBytes: Long, numFiles: Long, numberOfPartitions: Long, avgFileSizeInBytes:Double, avgPartitionSizeInBytes:Double, isZOrdered: Boolean, zOrderDetails: Seq[String])

// COMMAND ----------

/*
def isTablePartitioned(table:String): Boolean = {
    val df = spark.sql(s" describe ${table} ").filter($"col_name" === "Not partitioned")
    if (df.count == 1) {
      false
    }
    else {
      true
    }
}
*/
def isTablePartitioned(partCols:Seq[String]): Boolean = {
    if (partCols.size == 0) {
      false
    }
    else {
      true
    }
}

// COMMAND ----------

import org.apache.spark.sql.functions._
def getZOrderedDetails(table:String): Seq[String] = {
  var zorder_details = Seq[String]()
  val df = spark.sql(s" describe history ${table} ").filter($"operation" === "OPTIMIZE")
  if(df.count != 0) {
    val maxVersion = df.agg(max("version")).head.getLong(0)
    val zorder_str = df.filter($"version" === maxVersion).select(explode($"operationParameters")).filter($"key" === "zOrderBy").select("value").first().getAs[String](0).replaceAll("[\\[\\]]","").trim
    if(!zorder_str.equals("")) {
      zorder_details = zorder_str.split(",").map(_.trim).toSeq
    }
  }
  return zorder_details
}

// COMMAND ----------

def getTableDetails(table:String): Row = {
  spark.sql(s" describe detail ${table} ").first()
}

// COMMAND ----------

def getPartitionCount(table:String): Long ={
  spark.sql(s" show partitions ${table} ").count()
}
