# Pulling delta tables from CSVs unchanged
import dlt
from pyspark.sql import functions as F

bronze_path = "/Volumes/world_happiness_report/bronze/raw_data"

years = ["2015", "2016", "2017", "2018", "2019"]

def sanitize_column_name(name):
    return name.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "").replace(".", "_")

for year in years:
    def make_bronze_table(y):
        @dlt.table(
            name=f"world_happiness_bronze_{y}",
            comment=f"Raw World Happiness Report data for {y}"
        )
        def bronze_table():
            df = spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze_path}/{y}.csv")
            for col_name in df.columns:
                clean = sanitize_column_name(col_name)
                if clean != col_name:
                    df = df.withColumnRenamed(col_name, clean)
            return df
        return bronze_table

    make_bronze_table(year)
