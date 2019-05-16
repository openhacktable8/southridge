// Databricks notebook source
// Not sure which import i need
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// COMMAND ----------

// Create Southridge customers
var sr_customer_df = spark.read.json("mnt/adlsg2/cloudSales/Customers.txt")
// Add static column
sr_customer_df = sr_customer_df.withColumn("SourceID", lit(1))

// Add uniqueid from concatenated columns
sr_customer_df = sr_customer_df.withColumn("UniqueID", concat($"SourceID", $"CustomerID"))

// Order the columns
sr_customer_df = sr_customer_df.select(
  sr_customer_df("CustomerID"), 
  sr_customer_df("LastName"), 
  sr_customer_df("FirstName"), 
  sr_customer_df("PhoneNumber"), 
  sr_customer_df("CreatedDate").cast("timestamp"), 
  sr_customer_df("UpdatedDate").cast("timestamp"), 
  sr_customer_df("SourceID").cast("int"), 
  sr_customer_df("UniqueID")
  )

sr_customer_df.show()

// COMMAND ----------

// Create cloud streaming customers
var cs_customer_df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("mnt/adlsg2/cloudstreaming/dbo.Customers.txt")

// Add static column
cs_customer_df = cs_customer_df.withColumn("SourceID", lit(1))


// Add uniqueid from concatenated columns
cs_customer_df = cs_customer_df.withColumn("UniqueID", concat($"SourceID", $"CustomerID"))

//remove extra columns
cs_customer_df = cs_customer_df.select(
  cs_customer_df("CustomerID"), 
  cs_customer_df("LastName"), 
  cs_customer_df("FirstName"), 
  cs_customer_df("PhoneNumber"), 
  cs_customer_df("CreatedDate").cast("timestamp"), 
  cs_customer_df("UpdatedDate").cast("timestamp"), 
  cs_customer_df("SourceID").cast("int"), 
  cs_customer_df("UniqueID")
)


cs_customer_df.show()

// COMMAND ----------

// Van Arsdel
var va_customer_df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("mnt/adlsg2/VanArsdel/dbo.Customers.txt")

// Add static column
va_customer_df = va_customer_df.withColumn("SourceID", lit(2))


// Add uniqueid from concatenated columns
va_customer_df = va_customer_df.withColumn("UniqueID", concat($"SourceID", $"CustomerID"))

//remove extra columns
va_customer_df = va_customer_df.select(
  va_customer_df("CustomerID"), 
  va_customer_df("LastName"), 
  va_customer_df("FirstName"), 
  va_customer_df("PhoneNumber"), 
  va_customer_df("CreatedDate").cast("timestamp"), 
  va_customer_df("UpdatedDate").cast("timestamp"), 
  va_customer_df("SourceID").cast("int"), 
  va_customer_df("UniqueID")
)


va_customer_df.show()

// COMMAND ----------

// Fourth Coffee
var fc_customer_df = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("mnt/adlsg2/FourthCoffee/Customers.csv")

// Add static column
fc_customer_df = fc_customer_df.withColumn("SourceID", lit(3))


// Add uniqueid from concatenated columns
fc_customer_df = fc_customer_df.withColumn("UniqueID", concat($"SourceID", $"CustomerID"))

//remove extra columns
fc_customer_df = fc_customer_df.select(
  fc_customer_df("CustomerID"), 
  fc_customer_df("LastName"), 
  fc_customer_df("FirstName"), 
  fc_customer_df("PhoneNumber"), 
  fc_customer_df("CreatedDate").cast("timestamp"), 
  fc_customer_df("UpdatedDate").cast("timestamp"), 
  fc_customer_df("SourceID").cast("int"), 
  fc_customer_df("UniqueID")
)


fc_customer_df.show()

// COMMAND ----------

// Union all 3 data sets together

val all_customer_df = sr_customer_df.union(va_customer_df).union(fc_customer_df).union(cs_customer_df)

display(all_customer_df)

// COMMAND ----------

// Write back
all_customer_df.write.format("parquet").mode("overwrite").save("mnt/adlsg2/conformed/customers")
