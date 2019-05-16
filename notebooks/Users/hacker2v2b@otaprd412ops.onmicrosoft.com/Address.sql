-- Databricks notebook source
-- MAGIC %md
-- MAGIC #imports

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC import sqlContext.implicits._
-- MAGIC import org.apache.spark.sql._ 
-- MAGIC import org.apache.spark.sql.functions._

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # get raw data from extracts

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC var sourceBridgeAddressesDf = spark.read.json("mnt/adlsg2/cloudSales/Addresses.txt")
-- MAGIC 
-- MAGIC var vanArsdelAddressesDf = sqlContext.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("mnt/adlsg2/VanArsdel/dbo.Customers.txt")
-- MAGIC 
-- MAGIC var fourthCofeeAddressesDf = sqlContext.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("mnt/adlsg2/FourthCoffee/Customers.csv")
-- MAGIC 
-- MAGIC var cloudStreamingAddressesDf = sqlContext.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("mnt/adlsg2/cloudstreaming/dbo.Addresses.txt")
-- MAGIC   
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # add columns  

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC import org.apache.spark.sql.functions.expr
-- MAGIC 
-- MAGIC var uniqueIDExpression  = "concat(nvl(SourceID, ''), nvl(CustomerID, ''), nvl(AddressID, ''))"
-- MAGIC 
-- MAGIC sourceBridgeAddressesDf = sourceBridgeAddressesDf.withColumn("SourceID",lit(1))
-- MAGIC sourceBridgeAddressesDf = sourceBridgeAddressesDf.withColumn("UniqueID",expr(uniqueIDExpression)) 
-- MAGIC 
-- MAGIC vanArsdelAddressesDf = vanArsdelAddressesDf.withColumn("SourceID",lit(2)) 
-- MAGIC vanArsdelAddressesDf = vanArsdelAddressesDf.withColumn("AddressID",lit("") )
-- MAGIC vanArsdelAddressesDf = vanArsdelAddressesDf.withColumn("UniqueID",expr(uniqueIDExpression)) 
-- MAGIC  
-- MAGIC fourthCofeeAddressesDf = fourthCofeeAddressesDf.withColumn("SourceID",lit(3))
-- MAGIC fourthCofeeAddressesDf = fourthCofeeAddressesDf.withColumn("AddressID",lit("") )
-- MAGIC fourthCofeeAddressesDf = fourthCofeeAddressesDf.withColumn("UniqueID",expr(uniqueIDExpression))
-- MAGIC 
-- MAGIC cloudStreamingAddressesDf = cloudStreamingAddressesDf.withColumn("SourceID",lit(1)) 
-- MAGIC cloudStreamingAddressesDf = cloudStreamingAddressesDf.withColumn("UniqueID",expr(uniqueIDExpression))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # merge

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC sourceBridgeAddressesDf = sourceBridgeAddressesDf.select(
-- MAGIC                                   sourceBridgeAddressesDf("SourceID").cast("int").alias("SourceID"),
-- MAGIC                                   sourceBridgeAddressesDf("UniqueID"),
-- MAGIC                                   sourceBridgeAddressesDf("AddressID"),
-- MAGIC                                   sourceBridgeAddressesDf("AddressLine1"),
-- MAGIC                                   sourceBridgeAddressesDf("AddressLine2"),
-- MAGIC                                   sourceBridgeAddressesDf("City"),
-- MAGIC                                   sourceBridgeAddressesDf("State"),
-- MAGIC                                   sourceBridgeAddressesDf("ZipCode"),
-- MAGIC                                   sourceBridgeAddressesDf("CreatedDate").cast("timestamp"),
-- MAGIC                                   sourceBridgeAddressesDf("UpdatedDate").cast("timestamp") 
-- MAGIC                                  )
-- MAGIC   
-- MAGIC 
-- MAGIC vanArsdelAddressesDf = vanArsdelAddressesDf.select(
-- MAGIC                                   vanArsdelAddressesDf("SourceID").cast("int").alias("SourceID"),
-- MAGIC                                   vanArsdelAddressesDf("UniqueID"),
-- MAGIC                                   vanArsdelAddressesDf("AddressID"),
-- MAGIC                                   vanArsdelAddressesDf("AddressLine1"),
-- MAGIC                                   vanArsdelAddressesDf("AddressLine2"),
-- MAGIC                                   vanArsdelAddressesDf("City"),
-- MAGIC                                   vanArsdelAddressesDf("State"),
-- MAGIC                                   vanArsdelAddressesDf("ZipCode"),
-- MAGIC                                   vanArsdelAddressesDf("CreatedDate").cast("timestamp"),
-- MAGIC                                   vanArsdelAddressesDf("UpdatedDate").cast("timestamp") 
-- MAGIC                                  )
-- MAGIC fourthCofeeAddressesDf = fourthCofeeAddressesDf.select(
-- MAGIC                                   fourthCofeeAddressesDf("SourceID").cast("int").alias("SourceID"),
-- MAGIC                                   fourthCofeeAddressesDf("UniqueID"),
-- MAGIC                                   fourthCofeeAddressesDf("AddressID"),
-- MAGIC                                   fourthCofeeAddressesDf("AddressLine1"),
-- MAGIC                                   fourthCofeeAddressesDf("AddressLine2"),
-- MAGIC                                   fourthCofeeAddressesDf("City"),
-- MAGIC                                   fourthCofeeAddressesDf("State"),
-- MAGIC                                   fourthCofeeAddressesDf("ZipCode"),
-- MAGIC                                   fourthCofeeAddressesDf("CreatedDate").cast("timestamp"),
-- MAGIC                                   fourthCofeeAddressesDf("UpdatedDate").cast("timestamp") 
-- MAGIC                                  )
-- MAGIC cloudStreamingAddressesDf = cloudStreamingAddressesDf.select(
-- MAGIC                                   cloudStreamingAddressesDf("SourceID").cast("int").alias("SourceID"),
-- MAGIC                                   cloudStreamingAddressesDf("UniqueID"),
-- MAGIC                                   cloudStreamingAddressesDf("AddressID"),
-- MAGIC                                   cloudStreamingAddressesDf("AddressLine1"),
-- MAGIC                                   cloudStreamingAddressesDf("AddressLine2"),
-- MAGIC                                   cloudStreamingAddressesDf("City"),
-- MAGIC                                   cloudStreamingAddressesDf("State"),
-- MAGIC                                   cloudStreamingAddressesDf("ZipCode"),
-- MAGIC                                   cloudStreamingAddressesDf("CreatedDate").cast("timestamp"),
-- MAGIC                                   cloudStreamingAddressesDf("UpdatedDate").cast("timestamp") 
-- MAGIC                                  )
-- MAGIC  
-- MAGIC var mergedAddresses =  vanArsdelAddressesDf
-- MAGIC                         .union(sourceBridgeAddressesDf)
-- MAGIC                         .union(fourthCofeeAddressesDf)
-- MAGIC                         .union(cloudStreamingAddressesDf); 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # export

-- COMMAND ----------

-- MAGIC %scala  
-- MAGIC 
-- MAGIC 
-- MAGIC mergedAddresses 
-- MAGIC   .write
-- MAGIC   .format("parquet")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .save("mnt/adlsg2/conformed/addresses")