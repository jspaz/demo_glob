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

df_jo = spark.read.csv(
    path="/Volumes/main/default/files/jobs.csv",
    sep=",",
    header=True,
    inferSchema=True
)

# COMMAND ----------

@dlt.table(name="jobs")
@dlt.expect_or_drop(
    "valid_flield",
    "id IS NOT NULL and job IS NOT NULL",
)
def departments():
    return df_jo

# COMMAND ----------

df = spark.read.table("main.dlt.jobs")

# COMMAND ----------

df.write.format("avro").mode("overwrite").save("/Volumes/main/default/files/backup/jobs.avro")

# COMMAND ----------

df.write.jdbc(
    url=url,
    table="dbo.jobs",
    mode="overwrite",
    properties=properties
)
