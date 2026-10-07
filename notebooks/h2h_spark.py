# Databricks notebook source
from pyspark.sql import functions as F

tm = spark.table("footy.dev_marts.fct_team_match")          # 19,270 rows
teams = spark.table("footy.dev_marts.dim_team").select("team_key", "team_name")  # 160 rows
opps = teams.select(F.col("team_key").alias("opponent_key"),
                    F.col("team_name").alias("opponent_name"))

h2h = (
    tm.groupBy("team_key", "opponent_key")
      .agg(
          F.count("*").alias("matches"),
          F.sum(F.when(F.col("points") == 3, 1).otherwise(0)).alias("wins"),
          F.sum(F.when(F.col("points") == 1, 1).otherwise(0)).alias("draws"),
          F.sum(F.when(F.col("points") == 0, 1).otherwise(0)).alias("losses"),
          F.sum("goals_for").alias("goals_for"),
          F.sum("goals_against").alias("goals_against"),
          (F.sum("points") / F.count("*")).alias("points_per_game"),
      )
      .join(teams, "team_key")        # small dimension -> expect a broadcast join
      .join(opps, "opponent_key")
)

home_adv = (
    tm.groupBy("team_key", "div_code", "season")
      .pivot("is_home", [True, False])
      .agg(F.avg("points"))
      .withColumnRenamed("true", "ppg_home")
      .withColumnRenamed("false", "ppg_away")
      .withColumn("home_advantage", F.col("ppg_home") - F.col("ppg_away"))
)

(h2h.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("footy.dev_marts.h2h"))
(home_adv.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("footy.dev_marts.home_advantage"))

print("h2h:", spark.table("footy.dev_marts.h2h").count(),
      "| home_adv:", spark.table("footy.dev_marts.home_advantage").count())
h2h.explain(mode="formatted")
