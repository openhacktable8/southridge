# Databricks notebook source
# MAGIC %md
# MAGIC *** Mount ADLSg2 Drives

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("fs.azure.account.auth.type.southridgetable8.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.southridgetable8.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.southridgetable8.dfs.core.windows.net", "37f0a17a-1c0f-4967-b1fd-83b762d84f08")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.southridgetable8.dfs.core.windows.net","9OHzkj79oPhf9O:hqA=-I@]9qxch+10p")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.southridgetable8.dfs.core.windows.net", "https://login.microsoftonline.com/e90c0636-1f87-4976-8b51-79b66fcd6928/oauth2/token")
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
# MAGIC dbutils.fs.ls("abfss://southridgefs@southridgetable8.dfs.core.windows.net/")
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val configs = Map(
# MAGIC   "fs.azure.account.auth.type" -> "OAuth",
# MAGIC   "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC   "fs.azure.account.oauth2.client.id" -> "37f0a17a-1c0f-4967-b1fd-83b762d84f08", // Service Account ApplicationID
# MAGIC   "fs.azure.account.oauth2.client.secret" -> "9OHzkj79oPhf9O:hqA=-I@]9qxch+10p", // Service Account Key/Secret
# MAGIC   "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/e90c0636-1f87-4976-8b51-79b66fcd6928/oauth2/token") //Azure Active Directory - DirectoryID
# MAGIC 
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://southridgefs@southridgetable8.dfs.core.windows.net/",
# MAGIC   mountPoint = "/mnt/adlsg2",
# MAGIC   extraConfigs = configs)
# MAGIC  

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.mounts)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls("/databricks-results"))

# COMMAND ----------

from shutil import copyfile

copyfile(src, dst)


# COMMAND ----------

