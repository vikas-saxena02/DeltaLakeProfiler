// Databricks notebook source
val db_name = "test_vikas_framework"
val base_dir = "/tmp/vikas/framework"
val people_10m_location = base_dir + "people10m/"
val tbl_peopl10m = db_name + "." + "people10m"
val clickstream_unpartitioned_location = base_dir + "clickstream_unpartitioned"
val clickstream_partitioned_location = base_dir + "clickstream_partitioned"
val tbl_clickstream_unpartitioned = db_name + "." + "clickstream_unpartitioned"
val tbl_clickstream_partitioned = db_name + "." + "clickstream_partitioned"

// COMMAND ----------

dbutils.fs.rm(base_dir, true)

// COMMAND ----------

//display(dbutils.fs.ls(base_dir))

// COMMAND ----------

// MAGIC %md
// MAGIC # create database to store tables

// COMMAND ----------

spark.sql("create database if not exists " + db_name)

// COMMAND ----------

// MAGIC %md
// MAGIC # Create table -> unparitioned + optimized + no zordered

// COMMAND ----------

val df_people10m = spark.read.format("delta").load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

// COMMAND ----------

df_people10m.printSchema

// COMMAND ----------

df_people10m.write.format("delta").save(people_10m_location)

// COMMAND ----------

// Create the table.
spark.sql("CREATE TABLE " + tbl_peopl10m + " USING DELTA LOCATION '" + people_10m_location + "'")

// COMMAND ----------

spark.sql("optimize " + tbl_peopl10m)

// COMMAND ----------

// MAGIC %md
// MAGIC # create table -> unparitioned + non-optimised

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/"))

// COMMAND ----------

val df_clickstream = spark.read.format("json").load("/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/")

// COMMAND ----------

df_clickstream.printSchema

// COMMAND ----------

df_clickstream.show(false)

// COMMAND ----------

df_clickstream.write.format("delta").save(clickstream_unpartitioned_location)

// COMMAND ----------

// Create the table.
spark.sql("CREATE TABLE " + tbl_clickstream_unpartitioned + " USING DELTA LOCATION '" + clickstream_unpartitioned_location + "'")

// COMMAND ----------

// MAGIC %md
// MAGIC # create table -> paritioned + optimised + zordered

// COMMAND ----------

df_clickstream.printSchema

// COMMAND ----------

df_clickstream.count()

// COMMAND ----------

import org.apache.spark.sql.functions._
val df1_clickstream = df_clickstream.withColumn("date", lit("2020-07-25")) 

// COMMAND ----------

val df2_clickstream = df_clickstream.withColumn("date", lit("2020-07-26")) 
val df3_clickstream = df_clickstream.withColumn("date", lit("2020-07-27")) 
val df4_clickstream = df_clickstream.withColumn("date", lit("2020-07-28")) 

val df5_clickstream = df_clickstream.withColumn("date", lit("2020-07-29")) 
val df6_clickstream = df_clickstream.withColumn("date", lit("2020-07-30")) 
val df7_clickstream = df_clickstream.withColumn("date", lit("2020-07-31")) 

// COMMAND ----------

val df_clickstream_partitioned = df1_clickstream.union(df2_clickstream).union(df3_clickstream).union(df4_clickstream).union(df5_clickstream).union(df6_clickstream).union(df7_clickstream)

// COMMAND ----------

df_clickstream_partitioned.count()

// COMMAND ----------

df_clickstream_partitioned.write.partitionBy("date").mode("overwrite").format("delta").save(clickstream_partitioned_location)

// COMMAND ----------

// Create the table.
spark.sql("CREATE TABLE " + tbl_clickstream_partitioned + " USING DELTA LOCATION '" + clickstream_partitioned_location + "'")

// COMMAND ----------

spark.sql("optimize " + tbl_clickstream_partitioned + " zorder by curr_id")

// COMMAND ----------

display(dbutils.fs.ls(clickstream_partitioned_location))
