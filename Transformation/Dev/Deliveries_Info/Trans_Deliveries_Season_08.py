# Databricks notebook source
# MAGIC %md
# MAGIC ### %run --> include another notebook

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
# MAGIC ### Read Season_08 data from bronze_layer

# COMMAND ----------

df_1 = spark.read.format("delta").load("/FileStore/tables/bronze_layer/deliveries_info_data/season=8/")

df_1.createOrReplaceTempView("df_1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Adding Match Numbers that were having nulls

# COMMAND ----------

df_2 = sql("""SELECT *, 
              CASE 
                  WHEN match IS NULL AND match_date = "2015-05-19" THEN "57"
                  WHEN match IS NULL AND match_date = "2015-05-20" THEN "58"
                  WHEN match IS NULL AND match_date = "2015-05-22" THEN "59"
                  WHEN match IS NULL AND match_date = "2015-05-24" THEN "60"
                  ELSE match
              END AS match_no
              FROM df_1
           """)

df_2.createOrReplaceTempView("df_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT season, match, match_date
# MAGIC FROM df_2
# MAGIC WHERE season = "xxxxxxxxxx" OR match_no = "xxxxxxxxxx" OR match_date = "xxxxxxxxxx"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Final Dataframe with Required columns & Adding {ball} column

# COMMAND ----------

df_3 = sql("""SELECT CAST(season AS INT),
              CAST(match_date AS DATE),
              CAST(match_no AS INT),
              CAST(`over` AS INT),
              ROW_NUMBER() OVER(PARTITION BY `over`, match_date, team_playing ORDER BY `over`) AS ball,
              team_playing AS `team_batting`,
              batter_on_strike,
              non_striker AS `batter_non_strike`,
              bowler,
              batter AS `batter_on_strike_runs`,
              extras AS `extras_total`,
              CAST(COALESCE(wides, 0) AS INT) AS `extras_wide`,
              CAST(COALESCE(legbyes, 0) AS INT) AS `extras_legbye`,
              CAST(COALESCE(noballs, 0) AS INT) AS `extras_noball`,
              CAST(COALESCE(byes, 0) AS INT) AS `extras_bye`,
              CAST(COALESCE(penalty, 0) AS INT) AS `extras_penalty`,
              total AS team_runs,
              COALESCE(player_out, "N/A") AS player_out,
              COALESCE(kind, "N/A") AS kind,
              COALESCE(fielders_1, "N/A") AS fielder_involved_primary,
              COALESCE(was_substitute_1, False) AS was_substitute_primary,
              COALESCE(fielders_2, "N/A") AS fielder_involved_secondary,
              COALESCE(was_substitute_2, False) AS was_substitute_secondary,
              COALESCE(fielders_3, "N/A") AS fielder_involved_tertiary,
              COALESCE(was_substitute_3, False) AS was_substitute_tertiary
          FROM df_2
          ORDER BY `over`, match_no
         """)

display(df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Data to Silver Layer --> {dbfs:/FileStore/tables/silver_layer/deliveries_info_processed}

# COMMAND ----------

df_3.coalesce(1).write.mode("overwrite").format("delta").save(f"/FileStore/tables/silver_layer/deliveries_info_processed/{return_season()}/")

# COMMAND ----------

