# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # AI Agent: Silver -> Gold Curation + Dashboard
# MAGIC
# MAGIC Uses **Meta Llama 3.1 8B** (`databricks-meta-llama-3-1-8b-instruct`) to:
# MAGIC - Identify the most analytically important countries from the World Happiness silver table
# MAGIC - Generate key textual insights
# MAGIC - Write two new Gold tables: `gold_ai_curated` and `gold_ai_insights`
# MAGIC - Create a Lakeview dashboard visible in the Dashboards section

# COMMAND ----------

import json
import re
import urllib.request
import urllib.parse
import urllib.error
import pandas as pd

GATEWAY_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"
CATALOG          = "world_happiness_report"
SILVER_TABLE     = f"{CATALOG}.bronze.world_happiness_silver"
GOLD_SCHEMA      = f"{CATALOG}.bronze"

HOST  = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def ask_llm(prompt, max_tokens=900):
    body = json.dumps({
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
    }).encode()
    req = urllib.request.Request(
        f"{HOST}/serving-endpoints/{GATEWAY_ENDPOINT}/invocations",
        data=body,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        result = json.loads(r.read())
    raw = result["choices"][0]["message"]["content"].strip()
    raw = re.sub(r'^```(?:json)?\s*\n', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'\n```\s*$', '', raw, flags=re.MULTILINE)
    return raw.strip()


def db_api(method, path, body=None, params=None):
    url = f"{HOST}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode()}

# COMMAND ----------
# -- 1. Read Silver

silver_df = spark.read.table(SILVER_TABLE)
silver_pd = silver_df.toPandas()

print(f"Loaded {len(silver_pd):,} rows -- "
      f"{silver_pd['country'].nunique()} countries, "
      f"years {sorted(silver_pd['year'].unique())}")

# COMMAND ----------
# -- 2. Build LLM prompt

year_summary = (
    silver_pd.groupby("year")
    .agg(avg_score=("happiness_score", "mean"), n_countries=("country", "count"))
    .round(3)
    .reset_index()
    .to_dict(orient="records")
)

pivot = silver_pd.pivot(index="country", columns="year", values="happiness_rank").dropna()
if not pivot.empty and 2015 in pivot.columns and 2019 in pivot.columns:
    pivot["delta"] = pivot[2015] - pivot[2019]
    biggest_improvers = pivot.nlargest(5, "delta").index.tolist()
    biggest_decliners = pivot.nsmallest(5, "delta").index.tolist()
else:
    biggest_improvers, biggest_decliners = [], []

consistent_top = (
    silver_pd[silver_pd["happiness_rank"] <= 5]["country"]
    .value_counts()
    .head(8)
    .index.tolist()
)

prompt = f"""You are a data analyst for the World Happiness Report (2015-2019).

Year-level summary (avg score, country count):
{json.dumps(year_summary, indent=2)}

Countries with biggest rank improvement 2015->2019: {biggest_improvers}
Countries with biggest rank decline 2015->2019:     {biggest_decliners}
Consistently top-5 countries:                      {consistent_top}

Task: Select 15-20 countries that are most analytically important -- include consistent top performers, notable risers, notable decliners, and diverse regional examples.

Return ONLY valid JSON with this exact structure (no extra text):
{{
  "important_countries": ["Finland", "Norway", ...],
  "key_insights": [
    "Insight sentence 1.",
    "Insight sentence 2.",
    "Insight sentence 3.",
    "Insight sentence 4.",
    "Insight sentence 5."
  ],
  "reasoning": "One-line explanation of selection criteria."
}}"""

# COMMAND ----------
# -- 3. Call LLM

raw_content = ask_llm(prompt)
print("Raw LLM response:\n", raw_content[:500])

try:
    ai_result = json.loads(raw_content)
except json.JSONDecodeError:
    match = re.search(r"\{.*\}", raw_content, re.DOTALL)
    if match:
        ai_result = json.loads(match.group())
    else:
        raise ValueError(f"Could not parse JSON from LLM response:\n{raw_content}")

important_countries = ai_result.get("important_countries", [])
key_insights        = ai_result.get("key_insights", [])
valid_countries     = set(silver_pd["country"].unique())
important_countries = [c for c in important_countries if c in valid_countries]

print(f"\nLLM selected {len(important_countries)} valid countries:")
print(important_countries)
print(f"\nReasoning: {ai_result.get('reasoning', 'N/A')}")

# COMMAND ----------
# -- 4. Write gold_ai_curated

gold_curated_pd = silver_pd[silver_pd["country"].isin(important_countries)].copy()
gold_curated_pd["selected_by"] = GATEWAY_ENDPOINT

spark.createDataFrame(gold_curated_pd).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{GOLD_SCHEMA}.gold_ai_curated"
)
print(f"gold_ai_curated: {len(gold_curated_pd):,} rows, {gold_curated_pd['country'].nunique()} countries")

# COMMAND ----------
# -- 5. Write gold_ai_insights

insights_rows = [
    {"rank": i + 1, "insight": insight, "llm_model": GATEWAY_ENDPOINT}
    for i, insight in enumerate(key_insights)
]
spark.createDataFrame(insights_rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{GOLD_SCHEMA}.gold_ai_insights"
)
print(f"gold_ai_insights: {len(insights_rows)} insights written")

# COMMAND ----------
# -- 6. Create Lakeview Dashboard

warehouses = db_api("GET", "/api/2.0/sql/warehouses").get("warehouses", [])
WH_ID = warehouses[0]["id"] if warehouses else None
print(f"Using warehouse: {WH_ID}")

DASHBOARD_NAME = "World Happiness - AI Curated"

spec = {
    "datasets": [
        {
            "name": "ds_top_countries",
            "displayName": "Top Countries",
            "queryLines": [
                f"SELECT country, ROUND(AVG(happiness_score), 3) AS avg_score "
                f"FROM {GOLD_SCHEMA}.gold_ai_curated "
                f"GROUP BY country ORDER BY avg_score DESC LIMIT 10"
            ]
        },
        {
            "name": "ds_trends",
            "displayName": "Happiness Trends",
            "queryLines": [
                f"SELECT year, country, happiness_score "
                f"FROM {GOLD_SCHEMA}.gold_ai_curated "
                f"ORDER BY year, country"
            ]
        },
        {
            "name": "ds_factors",
            "displayName": "Happiness Factors",
            "queryLines": [
                f"SELECT year, "
                f"ROUND(AVG(gdp_per_capita),3) AS gdp_per_capita, "
                f"ROUND(AVG(social_support),3) AS social_support, "
                f"ROUND(AVG(health),3) AS health, "
                f"ROUND(AVG(freedom),3) AS freedom, "
                f"ROUND(AVG(generosity),3) AS generosity "
                f"FROM {GOLD_SCHEMA}.gold_ai_curated "
                f"GROUP BY year ORDER BY year"
            ]
        },
        {
            "name": "ds_insights",
            "displayName": "AI Insights",
            "queryLines": [
                f"SELECT rank, insight FROM {GOLD_SCHEMA}.gold_ai_insights ORDER BY rank"
            ]
        },
        {
            "name": "ds_count",
            "displayName": "Country Count",
            "queryLines": [
                f"SELECT COUNT(DISTINCT country) AS n_countries FROM {GOLD_SCHEMA}.gold_ai_curated"
            ]
        },
    ],
    "pages": [
        {
            "name": "Page_1",
            "displayName": "World Happiness",
            "pageType": "PAGE_TYPE_CANVAS",
            "layoutVersion": "GRID_V1",
            "layout": [
                {
                    "widget": {
                        "name": "w_count",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_count",
                            "fields": [{"name": "n_countries", "expression": "`n_countries`"}],
                            "disaggregated": True,
                        }}],
                        "spec": {
                            "version": 3,
                            "frame": {"showTitle": True},
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "n_countries"}
                            },
                            "data": {"queryName": "main_query"}
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "w_top_countries",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_top_countries",
                            "fields": [
                                {"name": "country", "expression": "`country`"},
                                {"name": "avg_score", "expression": "`avg_score`"},
                            ],
                            "disaggregated": True,
                        }}],
                        "spec": {
                            "version": 3,
                            "frame": {"showTitle": True},
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "country", "scale": {"type": "categorical"}},
                                "y": {"fieldName": "avg_score", "scale": {"type": "quantitative"}}
                            },
                            "data": {"queryName": "main_query"}
                        }
                    },
                    "position": {"x": 2, "y": 0, "width": 4, "height": 6}
                },
                {
                    "widget": {
                        "name": "w_trends",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_trends",
                            "fields": [
                                {"name": "year", "expression": "`year`"},
                                {"name": "country", "expression": "`country`"},
                                {"name": "happiness_score", "expression": "`happiness_score`"},
                            ],
                            "disaggregated": True,
                        }}],
                        "spec": {
                            "version": 3,
                            "frame": {"showTitle": True},
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "year", "scale": {"type": "quantitative"}},
                                "y": {"fieldName": "happiness_score", "scale": {"type": "quantitative"}},
                                "color": {"fieldName": "country", "scale": {"type": "categorical"}}
                            },
                            "data": {"queryName": "main_query"}
                        }
                    },
                    "position": {"x": 0, "y": 6, "width": 6, "height": 6}
                },
                {
                    "widget": {
                        "name": "w_factors",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_factors",
                            "fields": [
                                {"name": "year", "expression": "`year`"},
                                {"name": "gdp_per_capita", "expression": "`gdp_per_capita`"},
                                {"name": "social_support", "expression": "`social_support`"},
                                {"name": "health", "expression": "`health`"},
                                {"name": "freedom", "expression": "`freedom`"},
                                {"name": "generosity", "expression": "`generosity`"},
                            ],
                            "disaggregated": True,
                        }}],
                        "spec": {
                            "version": 3,
                            "frame": {"showTitle": True},
                            "widgetType": "table",
                            "data": {"queryName": "main_query"}
                        }
                    },
                    "position": {"x": 0, "y": 12, "width": 6, "height": 5}
                },
                {
                    "widget": {
                        "name": "w_insights",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_insights",
                            "fields": [
                                {"name": "rank", "expression": "`rank`"},
                                {"name": "insight", "expression": "`insight`"},
                            ],
                            "disaggregated": True,
                        }}],
                        "spec": {
                            "version": 3,
                            "frame": {"showTitle": True},
                            "widgetType": "table",
                            "data": {"queryName": "main_query"}
                        }
                    },
                    "position": {"x": 0, "y": 17, "width": 6, "height": 5}
                },
            ]
        }
    ],
    "uiSettings": {
        "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
        "applyModeEnabled": False
    }
}

existing = db_api("GET", "/api/2.0/lakeview/dashboards", params={"page_size": 50}).get("dashboards", [])
dash_id = next((d["dashboard_id"] for d in existing if d.get("display_name") == DASHBOARD_NAME), None)

if dash_id:
    r = db_api("PUT", f"/api/2.0/lakeview/dashboards/{dash_id}",
               body={"serialized_dashboard": json.dumps(spec)})
    if "error" in r:
        print(f"Update error: {r['error']}")
    else:
        print(f"Updated dashboard: {DASHBOARD_NAME}")
else:
    body = {"display_name": DASHBOARD_NAME, "serialized_dashboard": json.dumps(spec)}
    if WH_ID:
        body["warehouse_id"] = WH_ID
    result = db_api("POST", "/api/2.0/lakeview/dashboards", body=body)
    dash_id = result.get("dashboard_id")
    if "error" in result:
        print(f"Create error: {result['error']}")
    else:
        print(f"Created dashboard: {DASHBOARD_NAME} (id: {dash_id})")

if dash_id:
    pub_body = {"warehouse_id": WH_ID} if WH_ID else {}
    r = db_api("POST", f"/api/2.0/lakeview/dashboards/{dash_id}/published", body=pub_body)
    if "error" in r:
        print(f"Publish error: {r['error']}")
    else:
        print(f"Dashboard published successfully")

# COMMAND ----------

print("\nAI Agent completed successfully!")
print(f"  gold_ai_curated  -> {len(gold_curated_pd):,} rows, {gold_curated_pd['country'].nunique()} countries")
print(f"  gold_ai_insights -> {len(insights_rows)} insights")
print(f"  Dashboard        -> '{DASHBOARD_NAME}' (find it in Dashboards section)")
