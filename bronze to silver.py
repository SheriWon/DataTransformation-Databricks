# Databricks notebook source
# MAGIC %md
# MAGIC ## Doing transformation for one tables which located in Bronze file

# COMMAND ----------

dbutils.fs.ls('mnt/bronze/SalesLT/')

# COMMAND ----------

input_path =  '/mnt/bronze/SalesLT/Address/Address.parquet'

# COMMAND ----------

df = spark.read.format('parquet').load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType 

df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd")) #cahnge the date format in ModifiedDate field

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Doing transformation for all tables

# COMMAND ----------

table_name =[]

#using for each loop to iterate all tables in bronze folder, list all table name
for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split('/')[0])

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

#using for each loop to iterate all columns in the table
for i in table_name:
    path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'  #for instance /mnt/bronze/SalesLT/Address/Address.parquet
    df= spark.read.format('parquet').load(path)
    column = df.columns
    
    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
   
   #generate to delta save into silver folder
    output_path = '/mnt/silver/SalesLT/' +i +'/'
    df.write.format('delta').mode("overwrite").save(output_path)

display(df)



# COMMAND ----------


