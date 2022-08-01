# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Importing Necessary Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_1 = spark.read.format("delta").load("/FileStore/tables/silver_layer/deliveries_info_processed/season_1")

df_1.createOrReplaceTempView("df_1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Match Stats

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH temp1(team_batting, runs_scored, wicket, `over`)
# MAGIC AS(
# MAGIC    SELECT team_batting, 
# MAGIC           SUM(team_runs) AS runs_scored, 
# MAGIC           COUNT(DISTINCT player_out) - 1 AS wickets, 
# MAGIC           MAX(`over`) AS `over`
# MAGIC    FROM df_1
# MAGIC    WHERE match_no = 1            -- To be parameterized{Match_Number}
# MAGIC    GROUP BY team_batting
# MAGIC ),
# MAGIC temp2
# MAGIC AS(
# MAGIC    SELECT team_batting, `over`, ball, extras_wide, LAG(extras_wide) OVER(PARTITION BY team_batting ORDER BY team_batting) AS lag_row
# MAGIC    FROM df_1
# MAGIC    WHERE match_no = 1            -- To be parameterized{Match_Number}
# MAGIC    ORDER BY team_batting, match_date, `over`, ball
# MAGIC ),
# MAGIC temp3
# MAGIC AS(
# MAGIC    SELECT team_batting, 
# MAGIC            CASE
# MAGIC                WHEN lag_row >= 1 THEN ball - 1
# MAGIC                WHEN lag_row = 0 THEN ball
# MAGIC                ELSE ball
# MAGIC            END AS for_estimating_real_ball
# MAGIC    FROM temp2
# MAGIC ),
# MAGIC temp4(team_batting, ball)
# MAGIC AS(
# MAGIC    SELECT team_batting, LAST(for_estimating_real_ball) AS ball
# MAGIC    FROM temp3
# MAGIC    GROUP BY team_batting
# MAGIC ),
# MAGIC temp5(team_batting, runs_scored, wicket, `over`, ball)
# MAGIC AS(
# MAGIC    SELECT t1.*, t2.ball
# MAGIC    FROM temp1 t1
# MAGIC    LEFT JOIN temp4 t2
# MAGIC    ON t1.team_batting = t2.team_batting
# MAGIC )
# MAGIC SELECT team_batting AS team, runs_scored, wicket, `over`,
# MAGIC        CASE
# MAGIC            WHEN `ball` >= 6 THEN 6
# MAGIC            ELSE `ball`
# MAGIC        END AS `ball`,
# MAGIC        CASE
# MAGIC            WHEN `ball` >= 6 THEN FORMAT_NUMBER(ROUND(runs_scored/(`over` + 1), 1), 2)
# MAGIC            ELSE FORMAT_NUMBER((ROUND(runs_scored/CAST((CONCAT(`over`, '.', `ball`)) AS DOUBLE), 1)), 2)
# MAGIC        END AS `run_rate`
# MAGIC FROM temp5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH sixes_and_fours_1(batter_on_strike, `fours`, `sixes`)
# MAGIC AS(
# MAGIC    SELECT batter_on_strike,
# MAGIC           CASE WHEN batter_on_strike_runs = 4 THEN 1 ELSE 0 END AS `fours`,
# MAGIC           CASE WHEN batter_on_strike_runs = 6 THEN 1 ELSE 0 END AS `sixes`
# MAGIC    FROM df_1
# MAGIC    WHERE match_no = 1      -- To be parameterized{Match_Number}
# MAGIC ),
# MAGIC sixes_and_fours_2(batter_on_strike, `4S`, `6S`)
# MAGIC AS(
# MAGIC    SELECT batter_on_strike,
# MAGIC           SUM(fours) AS `4S`,
# MAGIC           SUM(sixes) AS `6S`
# MAGIC    FROM sixes_and_fours_1
# MAGIC    GROUP BY batter_on_strike
# MAGIC ),
# MAGIC scorecard_1(team, batsmen, runs_scored, balls_faced)
# MAGIC AS(
# MAGIC    SELECT team_batting AS team, 
# MAGIC                           batter_on_strike AS batsmen, 
# MAGIC                           SUM(batter_on_strike_runs) AS runs_scored, 
# MAGIC                           COUNT(batter_on_strike) AS balls_faced
# MAGIC    FROM df_1
# MAGIC    WHERE match_no = 1 AND extras_wide = 0    -- To be parameterized{Match_Number}
# MAGIC    GROUP BY batter_on_strike, team_batting
# MAGIC    ORDER BY team_batting
# MAGIC ),
# MAGIC out_method(batsmen, `out/not_out`)
# MAGIC AS(
# MAGIC    SELECT player_out AS batsmen,
# MAGIC           CASE
# MAGIC               WHEN kind = "run out" THEN CONCAT("run out (", fielder_involved_primary, "/", fielder_involved_secondary, ")")
# MAGIC               WHEN kind = "bowled" THEN CONCAT("b ", bowler)
# MAGIC               WHEN kind = "caught" THEN CONCAT("c ", fielder_involved_primary, " b ", bowler)
# MAGIC           END AS `out/not_out`
# MAGIC    FROM df_1
# MAGIC    WHERE match_no = 1 AND player_out <> "N/A"       -- To be parameterized{Match_Number}
# MAGIC )
# MAGIC SELECT s1.team,
# MAGIC        s1.batsmen,
# MAGIC        COALESCE(o.`out/not_out`, "not out") AS `out/not_out`,
# MAGIC        s1.runs_scored, 
# MAGIC        s1.balls_faced,
# MAGIC        FORMAT_NUMBER((s1.runs_scored/s1.balls_faced) * 100, 2) AS strike_rate,
# MAGIC        s2.`4S`, 
# MAGIC        s2.`6S`
# MAGIC FROM scorecard_1 s1
# MAGIC LEFT JOIN sixes_and_fours_2 s2
# MAGIC ON s1.batsmen = s2.batter_on_strike
# MAGIC LEFT JOIN out_method o
# MAGIC ON s1.batsmen = o.batsmen

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM df_1
# MAGIC WHERE match_no = 1
# MAGIC ORDER BY team_batting, match_date, `over`, ball

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM df_1
# MAGIC WHERE  match_no = 1 AND bowler = "LR Shukla" AND NOT extras_wide >= 1
# MAGIC --AND 1 NOT IN (extras_legbye)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT team_batting AS team,
# MAGIC        bowler,
# MAGIC        CASE
# MAGIC            WHEN COUNT(bowler) < 6 THEN CONCAT("0.", COUNT(bowler))
# MAGIC            WHEN COUNT(bowler) > 6 AND COUNT(bowler) < 12 THEN CONCAT("1.", COUNT(bowler) - 6)
# MAGIC            WHEN COUNT(bowler) > 12 AND COUNT(bowler) < 18 THEN CONCAT("2.", COUNT(bowler) - 12)
# MAGIC            WHEN COUNT(bowler) > 18 AND COUNT(bowler) < 24 THEN CONCAT("3.", COUNT(bowler) - 18)
# MAGIC            ELSE ROUND(COUNT(bowler) / 6, 0)
# MAGIC        END AS bowls_bowled
# MAGIC FROM df_1
# MAGIC WHERE match_no = 35 AND NOT extras_wide >= 1
# MAGIC GROUP BY bowler, team
# MAGIC ORDER BY team

# COMMAND ----------

