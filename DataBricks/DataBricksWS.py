# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access using ServicePrinciple(App)
# MAGIC

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.projectdatalakeacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.projectdatalakeacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.projectdatalakeacc.dfs.core.windows.net", "***************************")
spark.conf.set("fs.azure.account.oauth2.client.secret.projectdatalakeacc.dfs.core.windows.net", "*************************")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.projectdatalakeacc.dfs.core.windows.net", "https://login.microsoftonline.com/**********************************/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading files

# COMMAND ----------

df_cal = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Calender")

# COMMAND ----------

df_cus = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Customers")

# COMMAND ----------

df_procat = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Product_Categories")

# COMMAND ----------

df_prosub = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_pro = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Products")

# COMMAND ----------

df_ret = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Returns")

# COMMAND ----------

df_sales = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Sales*")

# COMMAND ----------

df_tet = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn("Year", year(col('Date')))\
         .withColumn("Month", month(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Calender")\
    .save()

# COMMAND ----------

df_tet.display()

# COMMAND ----------

df_tet.write.format("parquet")\
    .mode("append")\
    .save("abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Territories")

# COMMAND ----------

df_cus = df_cus.withColumn("Full Name", concat_ws(" ", df_cus.Prefix, df_cus.FirstName, df_cus.LastName))\
               .withColumn("AnnualIncomeinRupees", regexp_replace(regexp_replace(df_cus.AnnualIncome, ',', ''), '\\$', '').cast("double"))

# COMMAND ----------

df_cus = df_cus.withColumn("AnnualIncomeinRupees", df_cus.AnnualIncomeinRupees * 88.67)

# COMMAND ----------

df_cus.printSchema()

# COMMAND ----------

df_cus.write.format("parquet")\
    .mode("append")\
    .save("abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Customers")

# COMMAND ----------

if isinstance(df_cus, DataFrame):
    print("df_cus is a DataFrame")
else:
    print("df_cus is not a DataFrame")

# COMMAND ----------


df_procat = spark.read.format("csv")\
            .option("header", True)\
            .option("InferSchema", True)\
            .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Product_Categories")

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_procat.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Product_Categories")\
    .save()

# COMMAND ----------

df_prosub = spark.read.format("csv")\
            .option("header", True)\
            .option("InferSchema", True)\
            .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_prosub.display()

# COMMAND ----------

df_prosub.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Product_Subcategories")\
    .save()

# COMMAND ----------

df_pro = spark.read.format("csv")\
            .option("header", True)\
            .option("InferSchema", True)\
            .load("abfss://source@projectdatalakeacc.dfs.core.windows.net/Products")

# COMMAND ----------

df_pro = df_pro.withColumn("Profit", col("ProductPrice") - col("ProductCost"))


# COMMAND ----------

df_pro.write.format("parquet")\
    .mode("append")\
    .option("path", "abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Products")\
    .save()

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Returns")\
    .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format("parquet")\
    .mode("append")\
    .save("abfss://transformation@projectdatalakeacc.dfs.core.windows.net/Sales")

# COMMAND ----------

