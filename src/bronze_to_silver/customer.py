# Databricks notebook source
customer_df = spark.read.csv("dbfs:/mnt/bronze/customer", header=True)

# COMMAND ----------

from pyspark.sql.functions import split, when, col, to_date

# COMMAND ----------

def toSnakeCase(df):
    for column in df.columns:
        snake_case_col = column.replace(' ', '_').lower()  # Replace spaces with single underscores and convert to lower case
        df = df.withColumnRenamed(column, snake_case_col)
    return df

customer_df = toSnakeCase(customer_df)


# COMMAND ----------

# Split Name column into First Name and Last Name
customer_df = customer_df.withColumn("first_name", split(customer_df["name"], " ")[0])
customer_df = customer_df.withColumn("last_name", split(customer_df["name"], " ")[1])


# COMMAND ----------

# Extract domain from Email column
customer_df = customer_df.withColumn("domain", split(split(customer_df["email_id"], "@")[1], "\.")[0])



# COMMAND ----------

# Map gender to "M" and "F"
customer_df = customer_df.withColumn("gender", when(customer_df["gender"] == "male", "M").otherwise("F"))


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, date_format
#  Split Joining Date into Date and Time columns
customer_df = customer_df.withColumn("joining_date", to_timestamp(customer_df["joining_date"], "dd-MM-yyyy HH:mm"))
customer_df = customer_df.withColumn("date", date_format(customer_df["joining_date"], "yyyy-MM-dd"))
customer_df = customer_df.withColumn("time", date_format(customer_df["joining_date"], "HH:mm"))


# COMMAND ----------

# Create expenditure status column
customer_df = customer_df.withColumn("expenditure_status", when(customer_df["spent"] < 200, "MINIMUM").otherwise("MAXIMUM"))


# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://silver@storage1217.blob.core.windows.net/',
    mount_point='/mnt/silver',
    extra_configs={'fs.azure.account.key.storage1217.blob.core.windows.net':'2XmnRo5pnsJV25ZXoaQLn8U1y0i6XOowXOKcrkIPLwPqxoJK65kLHzbPxSzswZZ0DQI9hI9uGMQ/+ASt3WUrVA=='}
)


# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

writeto = f'dbfs:/mnt/silver/sales_view/customer'
write_delta_upsert(customer_df, writeto)

# COMMAND ----------

