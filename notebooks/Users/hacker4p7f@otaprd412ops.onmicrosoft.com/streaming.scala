// Databricks notebook source
// Not sure which import i need
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// COMMAND ----------

// get streaming data
// Create cloud streaming customers
var streaming_df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("mnt/adlsg2/cloudstreaming/dbo.Transactions.txt")

// Add static column
streaming_df = streaming_df.withColumn("SourceID", lit(1))

// Add uniqueid from concatenated columns
streaming_df = streaming_df.withColumn("UniqueOrderID", concat($"SourceID", $"TransactionID"))
streaming_df = streaming_df.withColumn("UniqueMovieID", concat($"SourceID", $"MovieID"))
streaming_df = streaming_df.withColumn("UniqueCustomerID", concat($"SourceID", $"CustomerID"))

//remove extra columns
streaming_df = streaming_df.select(
  streaming_df("SourceID").cast("int"),
streaming_df("UniqueOrderID"),
streaming_df("TransactionID"),
streaming_df("UniqueMovieID"),
streaming_df("MovieID"),
streaming_df("CustomerID"),
streaming_df("UniqueCustomerID"),
streaming_df("StreamStart").cast("timestamp"),
streaming_df("StreamEnd").cast("timestamp")
)

streaming_df.show()

// COMMAND ----------

// Write back
streaming_df.write.format("parquet").mode("overwrite").save("mnt/adlsg2/conformed/streaming")
