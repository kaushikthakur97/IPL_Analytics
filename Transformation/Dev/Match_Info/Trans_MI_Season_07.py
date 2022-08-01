# Databricks notebook source
# MAGIC %md
# MAGIC ### %run --> include another notebook with the helper functions

# COMMAND ----------

# MAGIC %run /Users/kaushikthakur152@gmail.com/Helper_Functions/Transformation/Match_Info/Helper_Trans_Match_Info

# COMMAND ----------

# MAGIC %run /Users/kaushikthakur152@gmail.com/Helper_Functions/General/Return_season_for_write

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
# MAGIC ### Read Season_07 data from bronze_layer

# COMMAND ----------

df_1 = spark.read.format("delta").load("/FileStore/tables/bronze_layer/match_info_data/season=7/")

df_1.createOrReplaceTempView("df_1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation of the required columns

# COMMAND ----------

df_2 = df_1.withColumn("season",col("season").cast(IntegerType()))\
           .withColumn("city_updated", when(df_1.venue == "Sharjah Cricket Stadium", "Sharjah")
                                      .when(df_1.venue == "Dubai International Cricket Stadium","Dubai")
                                      .when(df_1.venue == "Dubai International Cricket Stadium","Dubai")
                                      .otherwise(df_1.city))\
           .withColumn("venue", trans_venue(col("venue")))\
           .withColumn("reserve_umpire", handle_missing_values(col("reserve_umpire")))\
           .withColumn("match_referee", handle_missing_values(col("match_referee")))\
           .withColumn("tie", trans_tie(col("tie")).cast(IntegerType()))\
           .withColumn("outcome_by_runs", col("outcome_by_runs").cast(IntegerType()))\
           .withColumn("outcome_by_wickets", col("outcome_by_wickets").cast(IntegerType()))\
           .withColumn("date", when(df_1.date == "2014-05-27", "2014-05-28")
                                        .otherwise(df_1.date))\
           .withColumn("date",col("date").cast(DateType()))\
           .drop("revision", "city")

df_2.createOrReplaceTempView("df_2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Addition of match_number by using [ROW_NUMBER()]

# COMMAND ----------

df_3 = sql("""SELECT 
              season,
              ROW_NUMBER() OVER (ORDER BY `date`) match_no,
              city_updated AS city,
              venue,
              team_1,
              team_2,
              toss_winner,
              toss_decision,
              umpire_1,
              umpire_2,
              umpire_3,
              reserve_umpire,
              match_referee,
              winner AS winning_team,
              loser AS losing_team,
              COALESCE(tie, 0) tie,
              COALESCE(outcome_by_runs, -1) outcome_by_runs,
              COALESCE(outcome_by_wickets, -1) outcome_by_wickets,
              player_of_match,
              `date` AS match_date
              FROM df_2
              ORDER BY match_no
           """)

display(df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Data to Silver Layer --> {dbfs:/FileStore/tables/silver_layer/match_info_processed}

# COMMAND ----------

df_3.coalesce(1).write.mode("overwrite").format("delta").save(f"/FileStore/tables/silver_layer/match_info_processed/{return_season()}/")