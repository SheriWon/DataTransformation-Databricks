# Databricks notebook source
# MAGIC %md
# MAGIC ## data in gold folder for business reporting and consumption 

# COMMAND ----------

dbutils.fs.ls('mnt/silver/SalesLT')

# COMMAND ----------

input_path = '/mnt/silver/SalesLT/Address/'

# COMMAND ----------

df = spark.read.format('delta').load(input_path)
display(df)

# COMMAND ----------

#transforming header as 'Address_ID' format
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

#get the list of column names
column_names = df.columns

for old_col_name in column_names:
    # convert column name from ColumnNmar to Column_Name formatThe list comprehension checks each character in old_col_name.
    #If the character is uppercase (char.isupper()) and not preceded by another uppercase letter (not old_col_name[i - 1].isupper()), it prefixes it with an underscore ("_" + char).
    #otherwise, it keeps the character as is (else char)
    new_col_name ="".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

    #change the column name using withColumnRenamed and regexp_replace
    df =  df.withColumnRenamed(old_col_name, new_col_name)
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Doing transformation for all tables

# COMMAND ----------

table_name=[]

for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
    table_name.append(i.name.split('/')[0])
table_name

# COMMAND ----------

for name in table_name:
    path = '/mnt/silver/SalesLT/' + name 
    print(path)
    df = spark.read.format('delta').load(path)

    #get the list of column names
    column_names = df.columns 

    for old_col_name in column_names:
        #conver to column name from ColumnName to Column_Name format
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
        #change the column name using withColumnRenamed and regexp_replace
        df =  df.withColumnRenamed(old_col_name, new_col_name)
    
    output_path = '/mnt/gold/SalesLT/' +name + '/'
    df.write.format('delta').mode("overwrite").save(output_path)    
        

# COMMAND ----------


