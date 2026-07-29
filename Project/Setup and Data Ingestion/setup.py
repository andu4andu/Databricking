# File where it setups the needed structure (the catalog, schema, volume and layers)
from pyspark.sql import SparkSession

#Starting up Spark
spark = SparkSession.builder.getOrCreate()

#Configuring the names of the catalog and the volume
CATALOG = "world_happiness_report"
VOLUME = "raw_data"

#Creating the Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

#Creating the Schemas
spark.sql(f"create schema if not exists {CATALOG}.bronze")
spark.sql(f"create schema if not exists {CATALOG}.silver")
spark.sql(f"create schema if not exists {CATALOG}.gold")

#Creating the Volume
spark.sql(f"""
    create volume if not exists {CATALOG}.bronze.{VOLUME}
""")

#Checking if the Catalog was created

catalogs = spark.sql("SHOW CATALOGS").collect()
catalog_names = [row["catalog"] for row in catalogs]

if CATALOG in catalog_names:
    print(f"Catalaog '{CATALOG}' was created successfully")
else:
    print(f"Catalog '{CATALOG}' was NOT created")

#Checking if the Schemas were created

schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
schema_names = [row["databaseName"] for row in schemas]

for schema in ["bronze", "silver", "gold"]:
    if schema in schema_names:
        print(f"Schema '{schema}' was created successfully")
    else:
        print(f"Schema '{schema}' was NOT created")

