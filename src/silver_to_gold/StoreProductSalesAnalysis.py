# Databricks notebook source
# Databricks notebook source.
product_df='dbfs:/mnt/silver/sales_view/product'
store_df='dbfs:/mnt/silver/sales_view/store'

# COMMAND ----------

def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)

# COMMAND ----------

product_df = read_delta_file(product_df)
store_df = read_delta_file(store_df)


# COMMAND ----------

merged_product_store_df = product_df.join(store_df, product_df.store_id == store_df.store_id, "inner")
product_store_df = merged_product_store_df.select(store_df.store_id,"store_name","location","manager_name","product_id","product_name","product_code","description","category_id","price","stock_quantity","supplier_id",product_df.created_at.alias("product_created_at"),product_df.updated_at.alias("product_updated_at"),"image_url","weight","expiry_date","is_active","tax_rate")



# COMMAND ----------


customer_sales_df = read_delta_file('dbfs:/mnt/silver/sales_view/customer')


# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://gold@storage1217.blob.core.windows.net/',
    mount_point='/mnt/gold',
#    extra_configs={'fs.azure.account.key.storage1217.blob.core.windows.net':'2XmnRo5pnsJV25ZXoaQLn8U1y0i6XOowXOKcrkIPLwPqxoJK65kLHzbPxSzswZZ0DQI9hI9uGMQ/+ASt3WUrVA=='}
)

# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

writeto = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysi"
write_delta_upsert(product_store_df, writeto)

# COMMAND ----------

