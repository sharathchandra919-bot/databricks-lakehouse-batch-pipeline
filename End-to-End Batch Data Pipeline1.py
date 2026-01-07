# Databricks notebook source
silver_path = "/Volumes/workspace/default/streaming_checkpoint/nyc_yellow_taxi_trip_records_from_Jan_to_Aug_2023.csv"

df_raw = spark.read.format("csv").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(silver_path)

# df_raw.printSchema()
# df_raw.count()

# COMMAND ----------

from pyspark.sql.functions import col,to_timestamp,current_timestamp,year,month,row_number
from pyspark.sql.window import Window

df_std = (df_raw
          .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime")))
          .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime")))
          .withColumn("pickup_year", year(col("pickup_ts")))
          .withColumn("pickup_month", month(col("pickup_ts")))
          .withColumn("Vendor_id", col("VendorID").cast("integer"))
          .withColumn("pulocation_id",col("PULocationID").cast("int"))
          .withColumn("dolocation_id",col("DOLocationID").cast("int"))
          .withColumn("ingestion_ts", current_timestamp())
          .withColumnRenamed("_c0","id")
)

# df_std.show(5)
          
df_std.select("pickup_ts").count()
df_std.select("pickup_year", "pickup_month").distinct().orderBy(
    "pickup_year", "pickup_month"
).show()


# COMMAND ----------

# batch_year = 2023
# batch_month = 6

# dbutils.widgets.removeAll()

dbutils.widgets.text("batch_year", "2023")
dbutils.widgets.text("batch_month", "7")
batch_year = int(dbutils.widgets.get("batch_year"))
batch_month = int(dbutils.widgets.get("batch_month"))

df_batch = (df_std
            .filter((col("pickup_year") == batch_year) & (col("pickup_month") == batch_month))
)
display(df_batch)
df_batch.count()
# df_std.select("pickup_year", "pickup_month") \
#       .distinct() \
#       .orderBy("pickup_year", "pickup_month") \
#       .show()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.data;
# MAGIC -- SHOW VOLUMES IN workspace.default;
# MAGIC
# MAGIC

# COMMAND ----------

if df_batch.count() == 0:
  raise Exception(f"No data to process for {batch_year}-{batch_month}")


# COMMAND ----------

from pyspark.sql.window import Window
dedup = Window.partitionBy(
    "VendorID",
    "PULocationID",
    "DOLocationID"
).orderBy(col("pickup_ts").desc())

df_dedup = df_batch.withColumn("row_number", row_number().over(dedup)) \
        .filter(col("row_number") == 1).drop("row_number")


SILVER_PATH = "/Volumes/workspace/default/data/silver/silver_taxi"

CHECKPOINT_PATH = "/Volumes/workspace/default/checkpoints/silver_taxi"


df_dedup.write.format("delta").mode("append").partitionBy("pickup_year","pickup_month").save(SILVER_PATH)
df_dedup.write \
  .format("delta") \
  .mode("overwrite") \
  .partitionBy("pickup_year", "pickup_month") \
  .save(SILVER_PATH)



# COMMAND ----------

# df_dedup.count()
spark.read.format("delta").load(SILVER_PATH).count()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/data/silver/silver_taxi`;
# MAGIC
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/data/silver/silver_taxi`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/data/silver/silver_taxi`
# MAGIC ZORDER BY (VendorID, PULocationID, DOLocationID);
# MAGIC

# COMMAND ----------

#gold layer
from pyspark.sql.functions import col, sum, count

df_gold = spark.read.format("delta").load("/Volumes/workspace/default/data/silver/silver_taxi") \
    .groupBy("pickup_year", "pickup_month","PULocationID") \
    .agg(
        sum("total_amount").alias("total_revenue"),
        count("*").alias("total_trips")
    )
# df_gold.show(5)
# df_gold.count()

GOLD_PATH = "/Volumes/workspace/default/data/gold/gold_taxi"

df_gold.write \
    .mode("overwrite") \
    .format("delta") \
    .save(GOLD_PATH)

# COMMAND ----------

# spark.read.format("delta").load(GOLD_PATH).count()
df_gold.createOrReplaceTempView("gold_taxi")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/data/gold/gold_taxi` t
# MAGIC USING gold_taxi s
# MAGIC ON t.pickup_year = s.pickup_year AND t.PUlocationID = s.PUlocationID AND t.pickup_month = s.pickup_month
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

spark.read.format("delta").load("/Volumes/workspace/default/data/gold/gold_taxi").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/data/gold/gold_taxi`;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/data/gold/gold_taxi`
# MAGIC ZORDER BY (pickup_year, PUlocationID, pickup_month);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.default.gold_taxi
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/default/data/gold/gold_taxi`;
# MAGIC     
# MAGIC DESCRIBE TABLE workspace.default.gold_taxi;
# MAGIC