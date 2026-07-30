# Creating the Gold Tables!!!!!!
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="gold_top10_per_year",
    comment="Top 10 happiest countries per year"
)
def gold_top10_per_year():
    return (
        dlt.read("world_happiness_silver")
        .filter(F.col("happiness_rank") <= 10)
        .select("year", "country", "happiness_rank", "happiness_score")
        .orderBy("year", "happiness_rank")
    )

@dlt.table(
    name="gold_rankings_over_time",
    comment="Each country's happiness rank across all years"
)
def gold_rankings_over_time():
    return (
        dlt.read("world_happiness_silver")
        .select("country", "year", "happiness_rank", "happiness_score")
        .orderBy("country", "year")
    )

@dlt.table(
    name="gold_avg_score_per_year",
    comment="Average happiness score per year"
)
def gold_avg_score_per_year():
    return (
        dlt.read("world_happiness_silver")
        .groupBy("year")
        .agg(F.round(F.avg("happiness_score"), 3).alias("avg_happiness_score"))
        .orderBy("year")
    )

@dlt.table(
    name="gold_factor_averages",
    comment="Average of each happiness factor per year"
)
def gold_factor_averages():
    return (
        dlt.read("world_happiness_silver")
        .groupBy("year")
        .agg(
            F.round(F.avg("gdp_per_capita"), 3).alias("avg_gdp_per_capita"),
            F.round(F.avg("social_support"), 3).alias("avg_social_support"),
            F.round(F.avg("health"), 3).alias("avg_health"),
            F.round(F.avg("freedom"), 3).alias("avg_freedom"),
            F.round(F.avg("generosity"), 3).alias("avg_generosity"),
            F.round(F.avg("corruption"), 3).alias("avg_corruption")
        )
        .orderBy("year")
    )
