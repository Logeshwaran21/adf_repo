# Databricks notebook source
# Databricks notebook source file
store_df = spark.read.csv("dbfs:/mnt/bronze/store/20240105_sales_store.csv", header=True)


# COMMAND ----------


from pyspark.sql.functions import udf
def toSnakeCase(df):
    for column in df.columns:
        snake_case_col = ''
        for char in column:
            if char ==' ':
                snake_case_col += '_'
            else:
                snake_case_col += char.lower()
        df = df.withColumnRenamed(column, snake_case_col)
    return df

udf(toSnakeCase)
store_df=toSnakeCase(store_df)

# COMMAND ----------

from pyspark.sql.types import StringType

# COMMAND ----------

# Define UDF to extract store category from email address
def extract_store_category(email):
    return email.split('@')[1].split('.')[0]

# COMMAND ----------

# Register UDF
extract_store_category_udf = udf(extract_store_category, StringType())


# COMMAND ----------

# Add store category column
store_df = store_df.withColumn("store_category", extract_store_category_udf(store_df["email_address"]))


# COMMAND ----------

from pyspark.sql.functions import  from_unixtime

# COMMAND ----------

# Convert created_at and updated_at to yyyy-MM-dd format
store_df = store_df.withColumn("created_at", from_unixtime(store_df["created_at"], "yyyy-MM-dd"))
store_df = store_df.withColumn("updated_at", from_unixtime(store_df["updated_at"], "yyyy-MM-dd"))



# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

writeto = "dbfs:/mnt/silver/sales_view/store"
write_delta_upsert(store_df, writeto)

# COMMAND ----------

