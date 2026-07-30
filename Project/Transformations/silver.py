import dlt
import os
from pyspark.sql import functions as F

COLUMN_MAPS = {
    "2015": {
        "Country": "country",
        "Happiness Rank": "happiness_rank",
        "Happiness Score": "happiness_score",
        "Economy (GDP per Capita)": "gdp_per_capita",
        "Family": "social_support",
        "Health (Life Expectancy)": "health",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust (Government Corruption)": "corruption"
    },
    "2016": {
        "Country": "country",
        "Happiness Rank": "happiness_rank",
        "Happiness Score": "happiness_score",
        "Economy (GDP per Capita)": "gdp_per_capita",
        "Family": "social_support",
        "Health (Life Expectancy)": "health",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust (Government Corruption)": "corruption"
    },
    "2017": {
        "Country": "country",
        "Happiness.Rank": "happiness_rank",
        "Happiness.Score": "happiness_score",
        "Economy..GDP.per.Capita.": "gdp_per_capita",
        "Family": "social_support",
        "Health..Life.Expectancy.": "health",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust..Government.Corruption.": "corruption"
    },
    "2018": {
        "Country or region": "country",
        "Overall rank": "happiness_rank",
        "Score": "happiness_score",
        "GDP per capita": "gdp_per_capita",
        "Social support": "social_support",
        "Healthy life expectancy": "health",
        "Freedom to make life choices": "freedom",
        "Generosity": "generosity",
        "Perceptions of corruption": "corruption"
    },
    "2019": {
        "Country or region": "country",
        "Overall rank": "happiness_rank",
        "Score": "happiness_score",
        "GDP per capita": "gdp_per_capita",
        "Social support": "social_support",
        "Healthy life expectancy": "health",
        "Freedom to make life choices": "freedom",
        "Generosity": "generosity",
        "Perceptions of corruption": "corruption"
    }
}

COUNTRY_NAME_MAP = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Macedonia": "North Macedonia",
    "North Cyprus": "North Cyprus",
    "Northern Cyprus": "North Cyprus",
    "Somaliland region": "Somaliland Region",
    "Taiwan Province of China": "Taiwan",
    "Trinidad & Tobago": "Trinidad and Tobago"
}

STANDARD_COLUMNS = ["year", "country", "happiness_rank", "happiness_score",
                    "gdp_per_capita", "social_support", "health",
                    "freedom", "generosity", "corruption"]

bronze_path = "/Volumes/world_happiness_report/bronze/raw_data"

@dlt.table(
    name="world_happiness_silver",
    comment="Cleaned and standardized World Happiness Report data across all years"
)
def world_happiness_silver():
    all_dfs = []

    for filename in sorted(os.listdir(bronze_path)):
        year = filename.replace(".csv", "")
        mapping = COLUMN_MAPS.get(year)
        if not mapping:
            continue

        df = spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze_path}/{filename}")

        for old_col, new_col in mapping.items():
            df = df.withColumnRenamed(old_col, new_col)

        df = df.withColumn("year", F.lit(int(year))).select(STANDARD_COLUMNS)

        df = df.withColumn("country", F.trim(F.col("country")))

        # Standardize country names
        for wrong, correct in COUNTRY_NAME_MAP.items():
            df = df.withColumn("country", F.when(F.col("country") == wrong, correct).otherwise(F.col("country")))

        # Cast happiness_rank to integer
        df = df.withColumn("happiness_rank", F.col("happiness_rank").cast("integer"))

        # Round numeric columns to 3 decimal places
        for col_name in ["happiness_score", "gdp_per_capita", "social_support",
                         "health", "freedom", "generosity", "corruption"]:
            df = df.withColumn(col_name, F.round(F.col(col_name).cast("double"), 3))

        all_dfs.append(df)

    combined = all_dfs[0]
    for df in all_dfs[1:]:
        combined = combined.union(df)

    # Drop rows where core columns are null
    combined = combined.dropna(subset=["country", "happiness_score", "happiness_rank"])

    # Filter out invalid ranks
    combined = combined.filter(F.col("happiness_rank") >= 1)

    # Drop duplicate rows for the same country and year
    combined = combined.dropDuplicates(["country", "year"])

    return combined
