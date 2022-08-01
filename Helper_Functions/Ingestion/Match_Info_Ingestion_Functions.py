# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Check Season

# COMMAND ----------

season_year = {
               "2008": 1, "2009": 2,  "2010": 3,  "2011": 4,  "2012": 5,  "2013": 6,  "2014": 7,  
               "2015": 8, "2016": 9,  "2017": 10,  "2018": 11,  "2019": 12,  "2020": 13,  "2021": 14,  
               "2022": 15
              }

@udf("string")
def check_season(dict_info, file_name):
  if "dates" in dict_info:
    if len(dict_info["dates"]) >= 1:
      date = dict_info["dates"][0]
      year = date[0:4]
      if year in season_year:
        return season_year[year]
      else:
        return "Year not in keys"
    else:
      return "No values in Date field"
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Match

# COMMAND ----------

@udf("string")
def check_match(dict_info, file_name):
  if "event" in dict_info and "match_number" in dict_info["event"]:
    return dict_info["event"]["match_number"]
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check City

# COMMAND ----------

@udf("string")
def check_city(dict_info, file_name):
  if "city" in dict_info:
    return dict_info["city"]
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Venue

# COMMAND ----------

@udf("string")
def check_venue(dict_info, file_name):
  if "venue" in dict_info:
    return dict_info["venue"]
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Team

# COMMAND ----------

@udf("string")
def check_team(dict_info, file_name, team_number):
  team_number = int(team_number)
  if "teams" in dict_info and len(dict_info["teams"]) == 2:
    return dict_info["teams"][team_number]
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Toss {Winner, Decision}

# COMMAND ----------

@udf("string")
def check_toss_winner_decision(dict_info, file_name, toss_value):
  if "toss" in dict_info and toss_value in dict_info["toss"]:
    return dict_info["toss"][toss_value]
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Umpires {umpire_1, umpire_2, umpire_3, reserve_umpires, match_referee}

# COMMAND ----------

@udf("string")
def check_officials(dict_info, file_name, official, position):
  position = int(position)
  if "officials" in dict_info and official in dict_info["officials"]:
    try:
      return dict_info["officials"][official][position]
    except:
      return file_name
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Outcome {Winner, Loser}

# COMMAND ----------

@udf("string")
def check_outcome(dict_info, file_name, outcome):
  try: 
    teams = dict_info["teams"]  #Outputs ["Delhi Daredevils", "Pune Warriors"] 
    if "result" in dict_info["outcome"] and dict_info["outcome"]["result"] == "no result":
      return "no_result"
    elif "outcome" in dict_info and outcome == "winner":
      if "eliminator" in dict_info["outcome"]:
        return dict_info["outcome"]["eliminator"]
      else:
        return dict_info["outcome"]["winner"]
    elif "outcome" in dict_info and outcome == "loser":
      if "eliminator" in dict_info["outcome"]:
        winning_team = dict_info["outcome"]["eliminator"]
        teams.remove(winning_team)
        return teams[0]
      else:
        winning_team = dict_info["outcome"]["winner"]
        teams.remove(winning_team)
        return teams[0]
    else:
      return file_name
  except:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Tie

# COMMAND ----------

@udf("string")
def check_tie(dict_info, file_name):
  if "outcome" in dict_info:
    if "result" in dict_info["outcome"] and dict_info["outcome"]["result"] == "tie":
      return "tie"
    elif "result" in dict_info["outcome"] and dict_info["outcome"]["result"] == "no result":
      return "no_result"
    else:
      return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Outcome By {Runs, Wickets}

# COMMAND ----------

@udf("string")
def check_outcome_by(dict_info, file_name, outcome_by):
  if "outcome" in dict_info and "by" in dict_info["outcome"]:
    if "runs" in dict_info["outcome"]["by"] and outcome_by == "runs":
      return dict_info["outcome"]["by"]["runs"]
    elif "wickets" in dict_info["outcome"]["by"] and outcome_by == "wickets":
      return dict_info["outcome"]["by"]["wickets"]
    else:
      return "0"
  elif "outcome" in dict_info and "result" in dict_info["outcome"]:
    if "tie" in dict_info["outcome"]["result"]:
      return "tie"
    elif "no result" in dict_info["outcome"]["result"]:
      return "no_result"
    else:
      return file_name
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Player of the Match

# COMMAND ----------

@udf("string")
def check_player_of_the_match(dict_info, file_name):
  if "player_of_match" in dict_info:
    return dict_info["player_of_match"][0]
  elif "outcome" in dict_info and "result" in dict_info["outcome"]:
    if "no result" in dict_info["outcome"]["result"]:
      return "no_result"
  else:
    return file_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check Date

# COMMAND ----------

@udf("string")
def check_date(dict_info, file_name):
  try:
    if "dates" in dict_info:
      return dict_info["dates"][0]
  except:
    return file_name

# COMMAND ----------

