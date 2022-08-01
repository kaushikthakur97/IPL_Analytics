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
import pandas as pd
spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", True)

# COMMAND ----------

#Initializing empty RDD
emp_RDD = spark.sparkContext.emptyRDD()
 
# Defining the schema of the Columns_match_info_final DataFrame
schema_match_info_final = StructType([StructField('season', StringType(), False),
                                       StructField('match', StringType(), False),
                                       StructField('revision', StringType(), False),
                                       StructField('city', StringType(), False),
                                       StructField('venue', StringType(), False),
                                       StructField('team_1', StringType(), False),
                                       StructField('team_2', StringType(), False),
                                       StructField('toss_winner', StringType(), False),
                                       StructField('toss_decision', StringType(), False),
                                       StructField('umpire_1', StringType(), False),
                                       StructField('umpire_2', StringType(), False),
                                       StructField('umpire_3', StringType(), False),
                                       StructField('reserve_umpire', StringType(), False),
                                       StructField('match_referee', StringType(), False),
                                       StructField('winner', StringType(), False),
                                       StructField('loser', StringType(), False),
                                       StructField('tie', StringType(), False),
                                       StructField('outcome_by_runs', StringType(), False),
                                       StructField('outcome_by_wickets', StringType(), False),
                                       StructField('player_of_match', StringType(), False),
                                       StructField('date', StringType(), False)
                                      ])
#create empty Dataframe
match_info_final_df = spark.createDataFrame(data = emp_RDD, schema = schema_match_info_final)


path = "dbfs:/FileStore/tables/IPL_RAW_DATA/"


all_files = []
for path,file_name,size,modTime in dbutils.fs.ls(path):
  if file_name.endswith('.json'):
    all_files.append(file_name)
  

for individual_file in all_files:
  file_location = "dbfs:/FileStore/tables/IPL_RAW_DATA/" + individual_file
  

  df = spark.read.option("multiline","true")\
                 .option("inferSchema","true")\
                 .json(file_location)
  
  
  match_info = df.withColumn("season", check_season(lit(df.info), lit(individual_file)))\
                 .withColumn("match", check_match(lit(df.info), lit(individual_file)))\
                 .withColumn("revision", lit(df.meta.getItem("revision")))\
                 .withColumn("city", check_city(lit(df.info), lit(individual_file)))\
                 .withColumn("venue", check_venue(lit(df.info), lit(individual_file)))\
                 .withColumn("team_1", check_team(lit(df.info), lit(individual_file), lit("1")))\
                 .withColumn("team_2", check_team(lit(df.info), lit(individual_file), lit("0")))\
                 .withColumn("toss_winner", check_toss_winner_decision(lit(df.info), lit(individual_file), lit("winner")))\
                 .withColumn("toss_decision", check_toss_winner_decision(lit(df.info), lit(individual_file), lit("decision")))\
                 .withColumn("umpire_1", check_officials(lit(df.info), lit(individual_file), lit("umpires"), lit("0")))\
                 .withColumn("umpire_2", check_officials(lit(df.info), lit(individual_file), lit("umpires"), lit("1")))\
                 .withColumn("umpire_3", check_officials(lit(df.info), lit(individual_file), lit("tv_umpires"), lit("0")))\
                 .withColumn("reserve_umpire", check_officials(lit(df.info), lit(individual_file), lit("reserve_umpires"), lit("0")))\
                 .withColumn("match_referee", check_officials(lit(df.info), lit(individual_file), lit("match_referees"), lit("0")))\
                 .withColumn("winner", check_outcome(lit(df.info), lit(individual_file), lit("winner")))\
                 .withColumn("loser", check_outcome(lit(df.info), lit(individual_file), lit("loser")))\
                 .withColumn("tie", check_tie(lit(df.info), lit(individual_file)))\
                 .withColumn("outcome_by_runs", check_outcome_by(lit(df.info), lit(individual_file), lit("runs")))\
                 .withColumn("outcome_by_wickets", check_outcome_by(lit(df.info), lit(individual_file), lit("wickets")))\
                 .withColumn("player_of_match", check_player_of_the_match(lit(df.info), lit(individual_file)))\
                 .withColumn("date", check_date(lit(df.info), lit(individual_file)))\
                 .drop("meta", "info", "innings")
  
  # Append Match_Info data to empty DataFrame
  match_info_final_df = match_info_final_df.union(match_info).cache()

#cache the df
match_info_final_df.cache()

#Create Temp View
match_info_final_df.createOrReplaceTempView("match_info_final_df")

# display(match_info_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Data to Bronze Layer --> {dbfs:/FileStore/tables/Bronze_Layer/Match_Info} and partitioning by {season}

# COMMAND ----------

# Writing the data to Bronze Location
match_info_final_df.repartition(1).write.mode("overwrite").partitionBy("season").format("delta").save("/FileStore/tables/bronze_layer/match_info_data/")

# COMMAND ----------

