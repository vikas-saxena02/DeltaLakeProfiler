// Databricks notebook source
// DBTITLE 1,Util Functions
// MAGIC %scala
// MAGIC import java.sql.Timestamp
// MAGIC import com.databricks.sql.transaction.tahoe.util.FileNames
// MAGIC import com.databricks.sql.managedcatalog.ManagedCatalog
// MAGIC import com.databricks.sql.transaction.tahoe._
// MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
// MAGIC import com.databricks.spark.util.{DatabricksLogging, OpType, TagDefinition}
// MAGIC import com.databricks.spark.util.FrameProfiler // Edge
// MAGIC import com.databricks.spark.util.MetricDefinitions.{EVENT_LOGGING_FAILURE, EVENT_TAHOE}
// MAGIC import com.databricks.spark.util.TagDefinitions.{TAG_OP_TYPE, TAG_TAHOE_ID, TAG_TAHOE_PATH}
// MAGIC import com.databricks.sql.transaction.tahoe.DeltaLog
// MAGIC import com.databricks.sql.transaction.tahoe.util.DeltaProgressReporter
// MAGIC import com.databricks.sql.transaction.tahoe.util.DeltaProgressReporterEdge
// MAGIC import com.databricks.sql.transaction.tahoe.util.JsonUtils
// MAGIC
// MAGIC import org.apache.hadoop.fs.Path
// MAGIC
// MAGIC import org.apache.spark.SparkContext
// MAGIC import org.apache.spark.sql.SparkSession
// MAGIC
// MAGIC import java.util.concurrent.ForkJoinPool
// MAGIC import scala.util.Random
// MAGIC import scala.collection.parallel._
// MAGIC
// MAGIC case class TableDtl(
// MAGIC     var format: String,
// MAGIC     var id: String,
// MAGIC     var name: String,
// MAGIC     var description: String,
// MAGIC     var location: String,
// MAGIC     var createdAt: Timestamp,
// MAGIC     var lastModified: Timestamp,
// MAGIC     var partitionColumns: Seq[String],
// MAGIC     var zOrderColumns: String,
// MAGIC     var numFiles: java.lang.Long,
// MAGIC     var sizeInBytes: java.lang.Long,
// MAGIC     var properties: Map[String, String],
// MAGIC     var minReaderVersion: java.lang.Integer,
// MAGIC     var minWriterVersion: java.lang.Integer,
// MAGIC     var partitionCount: Long) {
// MAGIC
// MAGIC   def setPartitionCount(newPartitionCount: Long) {
// MAGIC     partitionCount = newPartitionCount
// MAGIC   }
// MAGIC }
// MAGIC
// MAGIC   def describeNonDeltaTable(table: CatalogTable): TableDtl = {
// MAGIC     var location = table.storage.locationUri.map(uri => CatalogUtils.URIToString(uri))
// MAGIC     // BEGIN-EDGE
// MAGIC     location = location.map(ManagedCatalog.restoreDecoratedTablePath)
// MAGIC     // END-EDGE
// MAGIC       new TableDtl(
// MAGIC         table.provider.orNull,
// MAGIC         null,
// MAGIC         table.qualifiedName,
// MAGIC         table.comment.getOrElse(""),
// MAGIC         location.orNull,
// MAGIC         new Timestamp(table.createTime),
// MAGIC         null,
// MAGIC         table.partitionColumnNames,
// MAGIC         null,
// MAGIC         null,
// MAGIC         null,
// MAGIC         table.properties,
// MAGIC         null,
// MAGIC         null,
// MAGIC         0L
// MAGIC       )
// MAGIC   }
// MAGIC
// MAGIC   def describeDeltaTable(
// MAGIC       sparkSession: SparkSession,
// MAGIC       deltaLog: DeltaLog,
// MAGIC       snapshot: Snapshot,
// MAGIC       tableMetadata: Option[CatalogTable]): TableDtl = {
// MAGIC     val currentVersionPath = FileNames.deltaFile(deltaLog.logPath, snapshot.version)
// MAGIC     val fs = currentVersionPath.getFileSystem(deltaLog.newDeltaHadoopConf())
// MAGIC     val tableName = tableMetadata.map(_.qualifiedName).getOrElse(snapshot.metadata.name)
// MAGIC     var location = deltaLog.dataPath.toString
// MAGIC
// MAGIC     val optimizeHistory = deltaLog.history.getHistory(0).filter(history => history.operation.equalsIgnoreCase("OPTIMIZE"))
// MAGIC     var zorderCols :String = null
// MAGIC     if(optimizeHistory.size > 0 ){
// MAGIC       var zOrderOption = optimizeHistory.head.operationParameters.get("zOrderBy")
// MAGIC       if(zOrderOption != null && zOrderOption.isDefined) {
// MAGIC         zorderCols = zOrderOption.get
// MAGIC       }
// MAGIC     }
// MAGIC     // BEGIN-EDGE
// MAGIC     location = ManagedCatalog.restoreDecoratedTablePath(location)
// MAGIC     // END-EDGE=
// MAGIC       new TableDtl(
// MAGIC         "delta",
// MAGIC         snapshot.metadata.id,
// MAGIC         tableName,
// MAGIC         snapshot.metadata.description,
// MAGIC         location,
// MAGIC         snapshot.metadata.createdTime.map(new Timestamp(_)).orNull,
// MAGIC         new Timestamp(fs.getFileStatus(currentVersionPath).getModificationTime),
// MAGIC         snapshot.metadata.partitionColumns,
// MAGIC         zorderCols,
// MAGIC         snapshot.numOfFiles,
// MAGIC         snapshot.sizeInBytes,
// MAGIC         snapshot.metadata.configuration,
// MAGIC         snapshot.protocol.minReaderVersion,
// MAGIC         snapshot.protocol.minWriterVersion,
// MAGIC       0L)
// MAGIC   }
// MAGIC
// MAGIC   def fetchTableDetails(sparkSession: SparkSession, catalog : Option[CatalogTable]): TableDtl = {
// MAGIC     val (basePath, tableMetadata) = (catalog.get.location, catalog)
// MAGIC     val deltaLog = DeltaLog.forTable(sparkSession, catalog.get)
// MAGIC     val snapshot = deltaLog.update()
// MAGIC     var tableDetail : TableDtl = null
// MAGIC     if (snapshot.version == -1) {
// MAGIC       tableDetail = describeNonDeltaTable(tableMetadata.get)
// MAGIC     } else {
// MAGIC       tableDetail = describeDeltaTable(sparkSession, deltaLog, snapshot, tableMetadata)
// MAGIC     }
// MAGIC     if(tableDetail.partitionColumns.size > 0) {
// MAGIC       tableDetail.setPartitionCount(sparkSession.sql("show partitions "+ tableDetail.name).count)
// MAGIC     }
// MAGIC     tableDetail
// MAGIC   }
// MAGIC
// MAGIC   class ParallellismHandler[A] {
// MAGIC     def withParallelism(list: List[A], n: Int) : scala.collection.parallel.immutable.ParSeq[A] = {
// MAGIC     val parList: scala.collection.parallel.immutable.ParSeq[A] = list.par
// MAGIC     parList.tasksupport = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(n))
// MAGIC     parList
// MAGIC   }
// MAGIC  }
// MAGIC