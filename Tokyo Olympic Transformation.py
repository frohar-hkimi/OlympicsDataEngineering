# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "ca851f31-4095-40c6-8ebd-edc88574ba1e",
"fs.azure.account.oauth2.client.secret": 'YIw8Q~oCDFHduMkZHsoNQhE-3RAD.3lm~~6tpbIf',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/1338728b-d46d-4f5c-8b08-08c264fd091f/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdatafrohar.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymicdata",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymicdata"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymicdata/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymicdata/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymicdata/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymicdata/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymicdata/raw-data/teams.csv")

# COMMAND ----------

athletes.show()`

# COMMAND ----------

athletes.printSchema()
coaches.printSchema()
entriesgender.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymicdata/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymicdata/raw-data/teams.csv")

# COMMAND ----------

teams.show()

# COMMAND ----------

# find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending = False).select("Team_Country", "Gold").show()

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymicdata/transformed-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymicdata/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymicdata/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymicdata/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymicdata/transformed-data/teams")
