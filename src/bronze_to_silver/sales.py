# Databricks notebook source
# Databricks notebook source file
sales_df = spark.read.csv("dbfs:/mnt/bronze/sales/20240105_sales_data.csv", header=True)


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
sales_df=toSnakeCase(sales_df)

# COMMAND ----------


def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

writeto = "dbfs:/mnt/silver/sales_view/sales"
write_delta_upsert(sales_df, writeto)

# COMMAND ----------

