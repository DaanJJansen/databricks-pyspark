# Databricks notebook source
from pyspark.sql.types import LongType, StringType, StructType, StructField, TimestampType, DecimalType,IntegerType
len(dbutils.fs.ls("wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.parquet"))
trxPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.parquet"
schema = StructType([StructField('transacted_at', TimestampType(), True), StructField('trx_id', StringType(), True), StructField('retailer_id', IntegerType(), True), StructField('description', StringType(), True), StructField('amount', DecimalType(38,2), True), StructField('city_id', IntegerType(), True)])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/cities/all.delta")

# COMMAND ----------

trxPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018_usa-only_45gb.delta"
citiesPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/cities/all.delta"

# COMMAND ----------

transactions_df = (spark
            .read
            .format("delta")
            .load(trxPath)
           )

cities_df = (spark
            .read
            .format("delta")
            .load(citiesPath)
           )

# COMMAND ----------

# transactions_df.write.format("delta").mode("overwrite").saveAsTable("globalSalesTransactions")
cities_df.write.format("delta").mode("overwrite").saveAsTable("globalCities")

# COMMAND ----------

transactions_df = spark.sql("select * from globalsalestransactions")
cities_df = spark.sql("select * from globalCities")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ANALYZE TABLE globalsalestransactions COMPUTE STATISTICS;
# MAGIC DESC EXTENDED globalsalestransactions;
# MAGIC
# MAGIC ANALYZE TABLE globalCities COMPUTE STATISTICS;
# MAGIC DESC EXTENDED globalCities;

# COMMAND ----------


# Join Datasets
from pyspark.sql.functions import broadcast

#create join
df_joined = transactions_df.join(broadcast(cities_df), on=transactions_df.city_id==cities_df.city_id, how="left_outer")

#write data
df_joined.write.format("noop").mode("overwrite").save()
# df_joined.explain()

# COMMAND ----------

sc.setJobDescription("Step A-1: Basic initialization")

# Disable the Delta IO Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", "false")               

# What is the maximum size of each spark-partition (default value)?
defaultMaxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b",""))

# What is the cost in bytes for each file (default value)?
openCostInBytes = int(spark.conf.get("spark.sql.files.openCostInBytes").replace("b",""))

display(f"MaxPartition: {defaultMaxPartitionBytes * 0.000001} MB")
display(f"openCostInBytes: {openCostInBytes * 0.000001} MB")


# COMMAND ----------

print(trxPath)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC sc.setJobDescription("Step C: Read at 1x")
# MAGIC
# MAGIC val defaultMaxPartitionBytes = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","").toLong
# MAGIC val trxPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.parquet"
# MAGIC val maxPartitionBytesConf = f"${defaultMaxPartitionBytes * 1}b"
# MAGIC val trxSchema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, new_at timestamp"
# MAGIC spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytesConf)
# MAGIC
# MAGIC // predictNumPartitions(trxFiles)
# MAGIC
# MAGIC spark.read.schema(trxSchema).parquet(trxPath)       // Load the transactions table
# MAGIC      .write.format("noop").mode("overwrite").save() // Test with a noop write

# COMMAND ----------


# spark.conf.get("spark.sql.cbo.enabled")
spark.conf.get("spark.sql.cbo.joinReorder.enabled")

# COMMAND ----------

spark.conf.set("spark.sql.cbo.enabled", True)
