# Databricks notebook source
# MAGIC %md
# MAGIC English SDK for Apache Spark
# MAGIC
# MAGIC Source: https://pyspark.ai/

# COMMAND ----------

# MAGIC %pip install pyspark-ai --upgrade
# MAGIC %pip install langchain-openai
# MAGIC %pip install pyspark-ai[ingestion]

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

from langchain_core.messages import HumanMessage
from langchain_openai import AzureChatOpenAI
from pyspark_ai import SparkAI


os.environ["AZURE_OPENAI_API_KEY"] = "176dafa6dc6e4243bbc3e9145d3e1553"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://myopenaisweden.openai.azure.com/"

model = AzureChatOpenAI(
    openai_api_version="2023-05-15",
    azure_deployment="gpt4",
)

spark_ai = SparkAI(llm=model)
spark_ai.activate()  # active partial functions for Spark DataFrame

# COMMAND ----------

message = HumanMessage(
    content="Translate this sentence from English to French. I love programming."
)
model([message])

# COMMAND ----------


df = spark_ai._spark.sql("SELECT * FROM samples.nyctaxi.trips")


# COMMAND ----------

df.ai.transform("What was the average trip distance for each day during the month of January 2016? Print the averages to the nearest tenth.").display()


# COMMAND ----------

auto_df = spark_ai.create_df("https://www.carpro.com/blog/full-year-2022-national-auto-sales-by-brand")

# COMMAND ----------

auto_df.show(n=5)

# COMMAND ----------

auto_df.ai.plot()
