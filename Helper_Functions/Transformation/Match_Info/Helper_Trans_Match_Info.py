# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Venue Transformation

# COMMAND ----------

@udf("String")
def trans_venue(venue):
  venue_1 = venue.find(",")
  if venue_1 != -1:
    venue_2 = venue[:venue_1]
    return venue_2
  elif venue_1 == -1:
    if venue.endswith("Stadium"):
      return venue
    elif "Stadium" in venue:
      position = venue.find("Stadium")
      venue_3 = venue[:position + 7]
      return venue_3
    else:
      return venue + " Stadium"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Handling Missing Values {reserve_umpire}

# COMMAND ----------

@udf("String")
def handle_missing_values(value):
  if value.endswith(".json"):
    return "no_data"
  else:
    return value

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Tie Transformation

# COMMAND ----------

@udf("String")
def trans_tie(tie_value):
  if tie_value.endswith(".json"):
    return "0"
  elif tie_value == "tie":
    return "1"
  else:
    return tie_value

# COMMAND ----------

