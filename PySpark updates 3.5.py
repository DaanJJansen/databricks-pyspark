# Databricks notebook source
# MAGIC %md
# MAGIC Arrow Optimized python UDF

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Arrow columnar format:
# MAGIC
# MAGIC <img src ='https://cms.databricks.com/sites/default/files/inline-images/db-751-blog-img-1.png' width="500" height="500">

# COMMAND ----------

spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", True)

#or

@udf("integer", useArrow=True)
def my_len_udf(s: str) -> int:
    return len(s)

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark user-defined table functions

# COMMAND ----------

from pyspark.sql.functions import udtf

class MyHelloUDTF:
    def eval(self, *args):
        yield "hello", "world"  



# COMMAND ----------

test_udtf = udtf(MyHelloUDTF, returnType="c1: string, c2: string")
test_udtf().show()

# COMMAND ----------

type(test_udtf)
