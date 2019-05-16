# Databricks notebook source
# MAGIC %scala 
# MAGIC display(dbutils.fs.ls("mnt/adlsg2/conformed/"))

# COMMAND ----------

import pandas as pd

t = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('mnt/adlsg2/FourthCoffee/Transactions.csv').toPandas()
t1 = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('mnt/adlsg2/VanArsdel/dbo.Transactions.txt').toPandas()

# COMMAND ----------

t['SourceID'] = 3
t['UniqueTransactionID'] = t['SourceID'].map(str)+t['TransactionID']
t['UniqueCustomerID'] = t['SourceID'].map(str)+t['CustomerID']
t['UniqueMovieID'] = t['SourceID'].map(str)+t['MovieID']
t.RewindFlag = t.RewindFlag.astype(bool)
t['RentalDate'] = pd.to_datetime(t['RentalDate'].astype(str), format='%Y%m%d')
t['ReturnDate'] = pd.to_datetime(t['ReturnDate'].astype(str), format='%Y%m%d')

# COMMAND ----------

t1['SourceID'] = 2
t1['UniqueTransactionID'] = t1['SourceID'].map(str)+t1['TransactionID']
t1['UniqueCustomerID'] = t1['SourceID'].map(str)+t1['CustomerID']
t1['UniqueMovieID'] = t1['SourceID'].map(str)+t1['MovieID']
t1.RewindFlag = t1.RewindFlag.astype(bool)
t1['RentalDate'] = pd.to_datetime(t1['RentalDate'].astype(str), format='%Y%m%d')
t1['ReturnDate'] = pd.to_datetime(t1['ReturnDate'].astype(str), format='%Y%m%d')

# COMMAND ----------

t3 = t.append(t1)

# COMMAND ----------

cols = t3.columns.tolist()
new_cols = ['SourceID', 'UniqueTransactionID', 'TransactionID', 'UniqueCustomerID', 'CustomerID', 'UniqueMovieID', 'MovieID', 'RentalDate', 'ReturnDate', 'RentalCost', 'LateFee', 'RewindFlag', 'CreatedDate', 'UpdatedDate']
t3 = t3[new_cols]

# COMMAND ----------

t4 = sqlContext.createDataFrame(t3)
t4.write.format("parquet").mode("overwrite").save("dbfs:/mnt/adlsg2/conformed/rentals")