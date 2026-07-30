import dlt
from pyspark.sql import functions as F

COLUMN_MAPS = {
    "2015": {
        "Country": "country",
        "Happiness_Rank": "happiness_rank",
        "Happiness_Score": "happiness_score",
        "Economy_GDP_per_Capita": "gdp_per_capita",
        "Family": "social_support",
        "Health_Life_Expectancy": "health",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust_Government_Corruption": "corruption"
    },
    "2016": {
        "Country": "country",
        "Happiness_Rank": "happiness_rank",
        "Happiness_Score": "happiness_score",
        "Economy_GDP_per_Capita": "gdp_per_capita",
        "Family": "social_support",
        "Health_Life_Expectancy": "health",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust_Government_Corruption": "corruption"
    },
    "2017": {
        "Country": "country",
        "Happiness_Rank": "happiness_rank",
        "Happiness_Score": "happiness_score",
        "Economy__GDP_per_Capita_": "gdp_per_capita",
        "Family": "social_support",
        "Health__Life_Expectancy_": "health",
        "Freedom": "freedom",
        "Generosity": "generosity",
        "Trust__Government_Corruption_": "corruption"
    },
    "2018": {
        "Country_or_region": "country",
        "Overall_rank": "happiness_rank",
        "Score": "happiness_score",
        "GDP_per_capita": "gdp_per_capita",
        "Social_support": "social_support",
        "Healthy_life_expectancy": "health",
        "Freedom_to_make_life_choices": "freedom",
        "Generosity": "generosity",
        "Perceptions_of_corruption": "corruption"
    },
    "2019": {
        "Country_or_region": "country",
        "Overall_rank": "happiness_rank",
        "Score": "happiness_score",
        "GDP_per_capita": "gdp_per_capita",
        "Social_support": "social_support",
        "Healthy_life_expectancy": "health",
        "Freedom_to_make_life_choices": "freedom",
        "Generosity": "generosity",
        "Perceptions_of_corruption": "corruption"
    }
}

COUNTRY_NAME_MAP = {
    "Hong Kong S.A.R., China": "Hong Kong",
    "Macedonia": "North Macedonia",
    "Northern Cyprus": "North Cyprus",
    "Somaliland region": "Somaliland Region",
    "Taiwan Province of China": "Taiwan",
    "Trinidad & Tobago": "Trinidad and Tobago"
}

STANDARD_COLUMNS = ["year", "country", "happiness_rank", "happiness_score",
                    "gdp_per_capita", "social_support", "health",
                    "freedom", "generosity", "corruption"]

years = ["2015", "2016", "2017", "2018", "2019"]

@dlt.table(
    name="world_happiness_silver",
    comment="Cleaned and standardized World Happiness Report data across all years"
)
def world_happiness_silver():
    all_dfs = []

    for year in years:
        mapping = COLUMN_MAPS.get(year)

        df = dlt.read(f"world_happiness_bronze_{year}")

        for old_col, new_col in mapping.items():
            df = df.withColumnRenamed(old_col, new_col)

        df = df.withColumn("year", F.lit(int(year))).select(STANDARD_COLUMNS)

        df = df.withColumn("country", F.trim(F.col("country")))

        for wrong, correct in COUNTRY_NAME_MAP.items():
            df = df.withColumn("country", F.when(F.col("country") == wrong, correct).otherwise(F.col("country")))

        df = df.withColumn("happiness_rank", F.col("happiness_rank").cast("integer"))

        for col_name in ["happiness_score", "gdp_per_capita", "social_support",
                         "health", "freedom", "generosity", "corruption"]:
            df = df.withColumn(col_name, F.round(F.col(col_name).cast("double"), 3))

        all_dfs.append(df)

    combined = all_dfs[0]
    for df in all_dfs[1:]:
        combined = combined.union(df)

    combined = combined.dropna(subset=["country", "happiness_score", "happiness_rank"])
    combined = combined.filter(F.col("happiness_rank") >= 1)
    combined = combined.dropDuplicates(["country", "year"])

    return combined
