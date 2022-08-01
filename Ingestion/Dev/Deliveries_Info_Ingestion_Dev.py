# Databricks notebook source
# MAGIC %md
# MAGIC ### %run --> include another notebook with the helper functions

# COMMAND ----------

# MAGIC %run /Users/kaushikthakur152@gmail.com/Helper_Functions/Ingestion/Match_Info_Ingestion_Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Importing Necessary Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read all Json Files

# COMMAND ----------

file_location = "dbfs:/FileStore/tables/IPL_RAW_DATA/*.json"

df = spark.read.option("multiline","true")\
                 .option("inferSchema","true")\
                 .json(file_location)\
                 .drop("meta")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Added season, match, match_date Columns and Exploding the array{innings}

# COMMAND ----------

df_1 = df.withColumn("season", check_season(lit(df.info), lit("xxxxxxxxxx")))\
         .withColumn("match", check_match(lit(df.info), lit("xxxxxxxxxx")))\
         .withColumn('Exp_RESULTS', explode(col('innings'))).drop('innings')\
         .withColumn("match_date", check_date(lit(df.info), lit("xxxxxxxxxx")))\
         .drop("info")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Adding Team Playing & Exploding the array{Overs}

# COMMAND ----------

df_2 = df_1.withColumn('team_playing', col("Exp_RESULTS").getItem("team"))\
           .withColumn('Exp_RESULTS_1', explode(df_1["Exp_RESULTS"].getItem("overs"))).drop('Exp_RESULTS')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Getting Over & Exploding the array{deliveries}

# COMMAND ----------

df_3 = df_2.withColumn("over", df_2["Exp_RESULTS_1"].getItem("over"))\
           .withColumn('Exp_RESULTS_2', explode(df_2["Exp_RESULTS_1"].getItem("deliveries"))).drop('Exp_RESULTS_1')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Selecting Required Columns & Changing name of the column{Exp_RESULTS_2.batter --> 'batter_on_strike'}

# COMMAND ----------

df_4 = df_3.selectExpr("season",\
                       "match",\
                       "over",\
                       "match_date",\
                       "team_playing",\
                       "Exp_RESULTS_2.batter AS batter_on_strike",\
                       "Exp_RESULTS_2.bowler",\
                       "Exp_RESULTS_2.extras.*",\
                       "Exp_RESULTS_2.non_striker",\
                       "Exp_RESULTS_2.runs.*",\
                       "Exp_RESULTS_2.wickets")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Exploding the array{wickets} & Selecting the columns one hirearchy down

# COMMAND ----------

df_5 = df_4.withColumn('wickets_new', explode_outer(df_4["wickets"])).drop("wickets")\
           .select("*", "wickets_new.*").drop("wickets_new")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Extracting the names of the fielders involved in dismissal of a player and check if they were a substitute player

# COMMAND ----------

df_6 = df_5.withColumn("fielders_1", col("fielders").getItem(0).getItem("name"))\
           .withColumn("was_substitute_1", col("fielders").getItem(0).getItem("substitute"))\
           .withColumn("fielders_2", col("fielders").getItem(1).getItem("name"))\
           .withColumn("was_substitute_2", col("fielders").getItem(1).getItem("substitute"))\
           .withColumn("fielders_3", col("fielders").getItem(2).getItem("name"))\
           .withColumn("was_substitute_3", col("fielders").getItem(2).getItem("substitute"))\
           .drop("fielders")

df_6.createOrReplaceTempView("df_6")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Data to Bronze Layer --> {dbfs:/FileStore/tables/Bronze_Layer/Deliveries_Info} and partitioning by {season}

# COMMAND ----------

# Writing the data to Bronze Location

df_6.repartition(1).write.mode("overwrite").partitionBy("season").format("delta").save("/FileStore/tables/bronze_layer/deliveries_info_data/")

# COMMAND ----------

