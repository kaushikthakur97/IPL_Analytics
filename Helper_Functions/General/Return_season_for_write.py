# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Rename file based on the season

# COMMAND ----------

def return_season():
  df_season = sql("SELECT DISTINCT season FROM df_1").collect()[0]
  for season in df_season:
    return "season_" + season

# COMMAND ----------

