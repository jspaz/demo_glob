# Databricks notebook source
import dlt

# COMMAND ----------

url = "jdbc:sqlserver://mx-demo.database.windows.net:1433;database=db-mx"
properties = {
    "user": "",
    "password": "",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "batchsize": "1000"
}

# COMMAND ----------

df_de = spark.read.csv(
    path="/Volumes/main/default/files/departments.csv",
    sep=",",
    header=True,
    inferSchema=True
)

# COMMAND ----------

@dlt.table(name="departments")
@dlt.expect_or_drop(
    "valid_flield",
    "id IS NOT NULL and department IS NOT NULL",
)
def jobs():
    return df_de

# COMMAND ----------

df = spark.read.table("main.dlt.departments")

# COMMAND ----------

df.write.format("avro").mode("overwrite").save("/Volumes/main/default/files/backup/departments.avro")

# COMMAND ----------

df.write.jdbc(
    url=url,
    table="dbo.departments",
    mode="overwrite",
    properties=properties
)
