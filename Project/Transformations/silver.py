# Silver layer - standardizes raw CSVs from bronze into a unified Delta table
# MAGIC %run "/Users/alexandruborduz@gmail.com/World Happiness/config"

import os
from datetime import datetime
from pyspark.sql import functions as F

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# Column mappings per year to standardize to common schema
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

STANDARD_COLUMNS = ["year", "country", "happiness_rank", "happiness_score",
                    "gdp_per_capita", "social_support", "health",
                    "freedom", "generosity", "corruption"]

bronze_path = f"/Volumes/{CONFIG['catalog']}/bronze/{CONFIG['volume']}"
silver_table = f"{CONFIG['catalog']}.silver.world_happiness"

all_dfs = []

for filename in sorted(os.listdir(bronze_path)):
    year = filename.replace(".csv", "")
    mapping = COLUMN_MAPS.get(year)
    if not mapping:
        log(f"No mapping found for {filename}, skipping.")
        continue

    log(f"Processing {filename}...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze_path}/{filename}")

    for old_col, new_col in mapping.items():
        df = df.withColumnRenamed(old_col, new_col)

    df = df.withColumn("year", F.lit(int(year))).select(STANDARD_COLUMNS)
    all_dfs.append(df)

# Union all years into one dataframe
combined = all_dfs[0]
for df in all_dfs[1:]:
    combined = combined.union(df)

# Count before writing to avoid an extra scan
total_rows = combined.count()

# Write to silver Delta table
combined.write.format("delta").mode("overwrite").saveAsTable(silver_table)
log(f"Silver table '{silver_table}' written with {total_rows} rows.")
