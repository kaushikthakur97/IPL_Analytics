# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Importing Necessary Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Initializing empty DF

# COMMAND ----------

#Initializing empty RDD
emp_RDD = spark.sparkContext.emptyRDD()
 
# Defining the schema of the Columns_match_info_final DataFrame
schema_match_info_final = StructType([StructField('season', IntegerType(), False),
                                       StructField('match_no', IntegerType(), False),
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
                                       StructField('winning_team', StringType(), False),
                                       StructField('losing_team', StringType(), False),
                                       StructField('tie', IntegerType(), False),
                                       StructField('outcome_by_runs', IntegerType(), False),
                                       StructField('outcome_by_wickets', IntegerType(), False),
                                       StructField('player_of_match', StringType(), False),
                                       StructField('date', DateType(), False)
                                      ])
#create empty Dataframe
match_info_processed_final_df = spark.createDataFrame(data = emp_RDD, schema = schema_match_info_final)

# COMMAND ----------

path = "dbfs:/FileStore/tables/silver_layer/match_info_processed/"

for path,file_name,size,modTime in dbutils.fs.ls(path):
    #Read Data
    df = spark.read.format("delta").load(path)
  
    #Append Match_Info data to empty DataFrame
    match_info_processed_final_df = match_info_processed_final_df.union(df)
  
#cache the df
match_info_processed_final_df.cache()

#Create Temp View
match_info_processed_final_df.createOrReplaceTempView("match_info_processed_final_df")

display(match_info_processed_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Changing 
# MAGIC ######- {Rising Pune Supergiants} --> {Rising Pune Supergiant} in required columns
# MAGIC ######- Venue and City Names to Original Names

# COMMAND ----------

match_info_processed_final_df = match_info_processed_final_df.withColumn('team_1', regexp_replace('team_1', 'Rising Pune Supergiants', 'Rising Pune Supergiant'))\
                                                             .withColumn('team_2', regexp_replace('team_2', 'Rising Pune Supergiants', 'Rising Pune Supergiant'))\
                                                             .withColumn('toss_winner', regexp_replace('toss_winner', 'Rising Pune Supergiants', 'Rising Pune Supergiant'))\
                                                             .withColumn('winning_team', regexp_replace('winning_team', 'Rising Pune Supergiants', 'Rising Pune Supergiant'))\
                                                             .withColumn('losing_team', regexp_replace('losing_team', 'Rising Pune Supergiants', 'Rising Pune Supergiant'))\
                                                             .withColumn('city', regexp_replace('city', 'Bangalore', 'Bengaluru'))\
                                                             .withColumn('city', regexp_replace('city', 'Navi Mumbai', 'Mumbai'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Zayed Cricket Stadium', 'Sheikh Zayed Stadium'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Sardar Patel Stadium', 'Narendra Modi Stadium'))\
                                                             .withColumn('venue', regexp_replace('venue', 'M.Chinnaswamy Stadium', 'M Chinnaswamy Stadium'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Feroz Shah Kotla Stadium', 'Arun Jaitley Stadium'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Subrata Roy Sahara Stadium', 'Maharashtra Cricket Association Stadium'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Eden Gardens Stadium', 'Eden Gardens'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Eden Gardens', 'Eden Gardens Stadium'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Dr DY Patil Sports Academy Stadium', 'Dr DY Patil Sports Academy'))\
                                                             .withColumn('venue', regexp_replace('venue', 'Dr DY Patil Sports Academy', 'Dr DY Patil Sports Academy Stadium'))\
                                 .withColumn('venue', regexp_replace('venue', 'Punjab Cricket Association IS Bindra Stadium', 'Punjab Cricket Association Stadium'))\
							     .withColumn('venue', regexp_replace('venue', 'Punjab Cricket Association Stadium', 'Punjab Cricket Association IS Bindra Stadium'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Data to Silver Layer --> {dbfs:/FileStore/tables/silver_layer/match_info_processed_final}

# COMMAND ----------

match_info_processed_final_df.coalesce(1).write.mode("overwrite").format("delta").save("/FileStore/tables/silver_layer/match_info_processed_final/")

# COMMAND ----------

