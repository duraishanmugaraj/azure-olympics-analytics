# Databricks notebook source
base_path_raw = "/mnt/olympic-analytics-data/raw/"

# COMMAND ----------

athletes = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path_raw + "Athletes.csv")
)
coaches = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path_raw + "Coaches.csv")
)
entriesgender = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path_raw + "EntriesGender.csv")
)
medals = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path_raw + "Medals.csv")
)
teams = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(base_path_raw + "Teams.csv")
)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

entriesgender = (
    entriesgender.withColumn("Female", col("Female").cast(IntegerType()))
    .withColumn("Male", col("Male").cast(IntegerType()))
    .withColumn("Total", col("Total").cast(IntegerType()))
)

# COMMAND ----------

top_gold_medal_countries = (
    medals.orderBy("Gold", ascending=False).select("TeamCountry", "Gold").show()
)

# COMMAND ----------

average_entries_by_gender = entriesgender.withColumn(
    "Avg_Female", entriesgender["Female"] / entriesgender["Total"]
).withColumn("Avg_Male", entriesgender["Male"] / entriesgender["Total"])
average_entries_by_gender.show()

# COMMAND ----------

base_path_transformaed = "/mnt/olympic-analytics-data/transformed/"

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header", "true").csv(
    base_path_transformaed + "athletes"
)
coaches.repartition(1).write.mode("overwrite").option("header", "true").csv(
    base_path_transformaed + "coaches"
)
entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv(
    base_path_transformaed + "entriesgender"
)
medals.repartition(1).write.mode("overwrite").option("header", "true").csv(
    base_path_transformaed + "medals"
)
teams.repartition(1).write.mode("overwrite").option("header", "true").csv(
    base_path_transformaed + "teams"
)

# COMMAND ----------

display(dbutils.fs.ls(base_path_transformaed))
