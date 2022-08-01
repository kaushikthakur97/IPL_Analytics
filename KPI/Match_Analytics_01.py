# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Importing Necessary Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_1 = spark.read.format("delta").load("/FileStore/tables/silver_layer/match_info_processed_final/")

# COMMAND ----------

df_2 = df_1.withColumn("season",df_1.season.cast('string'))\
           .withColumn("season", when(length(col("season")) == 1, concat(lit(0), col("season"))).otherwise(col("season")))\
           .withColumn("season", col("season").cast('int'))\
           .sort(col("season"))

df_2.createOrReplaceTempView("df_2")

# COMMAND ----------

display(df_2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Top Winning teams Ever

# COMMAND ----------

winning_team = sql("""
                      SELECT winning_team, COUNT(*) AS matches_won
                      FROM df_2
                      WHERE winning_team NOT IN ("match_abandoned", "no_result")
                      GROUP BY winning_team
                      ORDER BY matches_won DESC
                    """)

winning_team.createOrReplaceTempView("winning_team")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Top Losing teams Ever

# COMMAND ----------

losing_team = sql("""
                    SELECT losing_team, COUNT(*) AS matches_lost
                    FROM df_2
                    WHERE losing_team NOT IN ("match_abandoned", "no_result")
                    GROUP BY losing_team
                    ORDER BY matches_lost DESC
                  """)

losing_team.createOrReplaceTempView("losing_team")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Total Wins & Loss by Team Ever

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT wt.winning_team AS team, wt.matches_won, lt.matches_lost
# MAGIC FROM winning_team wt
# MAGIC INNER JOIN losing_team lt
# MAGIC ON wt.winning_team = lt.losing_team
# MAGIC ORDER BY matches_won DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Top Winning teams Seasonwise with Match Points

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT season, winning_team, COUNT(*) AS matches_won, (COUNT(*) * 2) AS points
# MAGIC FROM df_2
# MAGIC WHERE winning_team NOT IN ("match_abandoned", "no_result")
# MAGIC GROUP BY winning_team, season
# MAGIC ORDER BY season ASC, matches_won DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Number of Teams Season Wise

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT season, COUNT(DISTINCT team_1) AS number_of_teams
# MAGIC FROM df_2
# MAGIC GROUP BY season
# MAGIC ORDER BY season

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Toss Winner - Highest to Lowest

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH 
# MAGIC temp1(toss_winner, toss_decision, toss_decision_no_of_times)
# MAGIC AS(
# MAGIC     SELECT toss_winner, toss_decision, COUNT(toss_decision) AS toss_decision_no_of_times
# MAGIC     FROM df_2
# MAGIC     WHERE toss_winner NOT IN ("match_abandoned", "no_result") AND toss_decision NOT IN ("match_abandoned", "no_result")
# MAGIC     GROUP BY toss_winner, toss_decision
# MAGIC     ORDER BY toss_winner, toss_decision
# MAGIC ),
# MAGIC temp2(toss_winner, toss_win_bat, toss_win_field)
# MAGIC AS(
# MAGIC     SELECT toss_winner,
# MAGIC            SUM(CASE WHEN toss_decision = "bat" THEN toss_decision_no_of_times END) AS toss_win_bat,
# MAGIC            SUM(CASE WHEN toss_decision = "field" THEN toss_decision_no_of_times END) AS toss_win_field
# MAGIC     FROM temp1
# MAGIC     GROUP BY toss_winner
# MAGIC )
# MAGIC SELECT toss_winner, (toss_win_bat + toss_win_field) AS times_won, toss_win_bat, toss_win_field FROM temp2
# MAGIC ORDER BY times_won DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Most Man of the Match

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT player_of_match AS player_of_the_match, COUNT(player_of_match) AS number_of_times, 
# MAGIC        concat_ws(" | ", collect_list(concat(match_no, "-", season))) AS `awarded_in_match-season`
# MAGIC FROM df_2
# MAGIC WHERE player_of_match NOT IN ("match_abandoned", "no_result")
# MAGIC GROUP BY player_of_match
# MAGIC ORDER BY number_of_times DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Most Matches Played by {City}

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT city,  COUNT(venue) AS `total_matches_played`
# MAGIC FROM df_2
# MAGIC GROUP BY city
# MAGIC ORDER BY `total_matches_played` DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Most Matches Played by {Venue}

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT city, venue, COUNT(venue) AS `total_matches_played`
# MAGIC FROM df_2
# MAGIC GROUP BY city, venue
# MAGIC ORDER BY total_matches_played DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Venue Level Stats

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH most_matches_played_by_venue(city, venue, total_matches_played)
# MAGIC AS(
# MAGIC   SELECT city, venue, COUNT(venue) AS `total_matches_played`
# MAGIC   FROM df_2
# MAGIC   GROUP BY city, venue
# MAGIC   ORDER BY total_matches_played DESC
# MAGIC ),
# MAGIC temp1(venue, toss_decision, number_of_times_chosen)
# MAGIC AS(
# MAGIC    SELECT venue, toss_decision, COUNT(toss_decision) AS `number_of_times_chosen`
# MAGIC    FROM df_2
# MAGIC    GROUP BY venue, toss_decision
# MAGIC    ORDER BY venue
# MAGIC ),
# MAGIC temp2(venue, toss_win_bat, toss_win_field, match_abandoned)
# MAGIC AS(
# MAGIC    SELECT venue, 
# MAGIC    COALESCE(SUM(CASE WHEN toss_decision = "bat" THEN number_of_times_chosen END), 0) AS toss_win_bat,
# MAGIC    COALESCE(SUM(CASE WHEN toss_decision = "field" THEN number_of_times_chosen END), 0) AS toss_win_field,
# MAGIC    COALESCE(SUM(CASE WHEN toss_decision = "match_abandoned" THEN number_of_times_chosen END), 0) AS match_abandoned
# MAGIC    FROM temp1
# MAGIC    GROUP BY venue
# MAGIC    ORDER BY venue
# MAGIC ),
# MAGIC temp3(venue, total_matches, toss_times, batting_happened_first, fielding_happened_first, match_abandoned)
# MAGIC AS(
# MAGIC    SELECT venue,
# MAGIC    (SUM(toss_win_bat + toss_win_field + match_abandoned)) AS total_matches,
# MAGIC    (SUM(toss_win_bat + toss_win_field)) AS toss_times,
# MAGIC    toss_win_bat AS batting_happened_first,
# MAGIC    toss_win_field AS fielding_happened_first,
# MAGIC    match_abandoned
# MAGIC    FROM temp2
# MAGIC    GROUP BY venue, toss_win_bat, toss_win_field, match_abandoned
# MAGIC )
# MAGIC SELECT a.*, b.match_abandoned AS matches_abandoned , b.toss_times, b.batting_happened_first, b.fielding_happened_first
# MAGIC FROM most_matches_played_by_venue a
# MAGIC INNER JOIN temp3 b
# MAGIC ON a.venue = b.venue
# MAGIC ORDER BY total_matches_played DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM df_2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Umpire Level Stats

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH temp1(umpire)
# MAGIC AS(
# MAGIC    SELECT umpire_1 FROM df_2
# MAGIC    UNION ALL
# MAGIC    SELECT umpire_2 FROM df_2
# MAGIC ),
# MAGIC temp2(umpire_onfield, as_primary_umpire)
# MAGIC AS(
# MAGIC    SELECT umpire, COUNT(*) AS as_primary_umpire
# MAGIC    FROM temp1
# MAGIC    WHERE umpire NOT IN ("match_abandoned")
# MAGIC    GROUP BY umpire
# MAGIC    ORDER BY as_primary_umpire DESC
# MAGIC ),
# MAGIC temp3(umpire_tv, as_third_umpire)
# MAGIC AS(
# MAGIC    SELECT umpire_3, COUNT(umpire_3) AS as_third_umpire
# MAGIC    FROM df_2
# MAGIC    WHERE umpire_3 NOT IN ("1254088.json", "598021.json")
# MAGIC    GROUP BY umpire_3
# MAGIC    ORDER BY as_third_umpire DESC
# MAGIC ),
# MAGIC temp4(umpire_reserve, as_reserve_umpire)
# MAGIC AS(
# MAGIC    SELECT reserve_umpire, COUNT(reserve_umpire) AS as_reserve_umpire
# MAGIC    FROM df_2
# MAGIC    WHERE reserve_umpire NOT IN ("1254088.json", "598021.json")
# MAGIC    GROUP BY reserve_umpire
# MAGIC    ORDER BY as_reserve_umpire DESC
# MAGIC )
# MAGIC SELECT umpire_onfield AS umpire, as_primary_umpire, COALESCE(as_third_umpire, 0) AS as_third_umpire, COALESCE(as_reserve_umpire, 0) AS as_reserve_umpire
# MAGIC FROM temp2
# MAGIC LEFT JOIN temp3
# MAGIC ON temp2.umpire_onfield = temp3.umpire_tv
# MAGIC LEFT JOIN temp4
# MAGIC ON temp2.umpire_onfield = temp4.umpire_reserve
# MAGIC ORDER BY as_primary_umpire DESC

# COMMAND ----------

