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

df_he = spark.read.csv(
    path="/Volumes/main/default/files/hired_employees.csv",
    sep=",",
    header=True,
    inferSchema=True
)

# COMMAND ----------

@dlt.table(name="hired_employees")
@dlt.expect_or_drop(
    "valid_flield",
    "id IS NOT NULL and name IS NOT NULL and datetime IS NOT NULL and department_id IS NOT NULL and job_id IS NOT NULL",
)
def tabla_hired_employees():
    return df_he

# COMMAND ----------

df = spark.read.table("main.dlt.hired_employees")

# COMMAND ----------

df.write.format("avro").mode("overwrite").save("/Volumes/main/default/files/backup/hired_employees.avro")

# COMMAND ----------

df.write.jdbc(
    url=url,
    table="dbo.hired_employees",
    mode="overwrite",
    properties=properties
)
