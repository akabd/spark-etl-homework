# Spark ETL: Restaurants × Weather Enrichment

This project implements an end-to-end **Spark ETL pipeline** that enriches restaurant data with monthly weather statistics based on geospatial proximity (geohash).

---

## Architecture Overview

**Input**
- Restaurants CSV files 
- Weather data in Parquet format, partitioned by year/month/day

**Processing**
1. Read restaurants data
2. Compute geohash4 from latitude/longitude
3. Resolve missing coordinates via OpenCage Geocoding API
4. Recompute geohash after enrichment
5. Read multiple months of weather data
6. Deduplicate weather data to monthly level (to avoid join multiplication)
7. Expand restaurants across `(year, month)`
8. Left join restaurants with weather
9. Write enriched dataset as partitioned Parquet

**Output**
- Partitioned Parquet dataset: out/enriched/year=YYYY/month=MM/country=XX/


---

## Project Structure

```text
spark-etl-homework/
├── data/
│   ├── restaurants/
│   └── weather/
│
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── job.py            # Spark entrypoint
│       ├── io.py             # Data readers
│       ├── geocode.py        # OpenCage integration
│       ├── geohash_udf.py    # Geohash UDF
│       └── enrich.py         # Core transformations
│
├── tests/
│   ├── test_geohash.py
│   ├── test_geocode_mapping.py
│   └── test_join_idempotent.py
│
├── requirements.txt
├── pytest.ini
└── README.md
```
---
## Weather Data Preparation

The original weather dataset is provided in a fragmented layout, where a single month
is split across multiple folders (e.g. `*_October_*`), each containing a subset of days:
```bash
data/weather/
├── 1_October_1-4/
│ └── weather/year=2016/month=10/day=01
├── 2_October_5-8/
│ └── weather/year=2016/month=10/day=05
├── ...
```

For simplicity and to make the ETL pipeline deterministic and easier to reason about,
the dataset was **preprocessed** by consolidating all daily partitions of the same
`year-month` into a single directory:
```bash
data/weather/2016-10/
├── day=01
├── day=02
├── ...
├── day=31
```
The same normalization was applied for:
- `2017-08`
- `2017-09`

    'data/' is excluded from Git; dataset must be prepared locally as described above
---
## Main functions 
1) **Geohash UDF (src/etl/geohash_udf.py).** 

    Goal: compute geohash4 from (lat, lng)
```python
def _geohash4(lat, lng):
    if lat is None or lng is None:
        return None
    try:
        return pgh.encode(float(lat), float(lng), precision=4)
    except Exception:
        return None
```

2) **Geocoding missing coordinates (src/etl/geocode.py).** 
    
    We request geocoding only for distinct (city, country) pairs where coordinates are missing/invalid.
This reduces API calls and makes enrichment deterministic: same (city, country) ⇒ same resolved coords.
```python
def build_missing_coords_mapping(df, api_key: str, country_code: str | None = None, rate_limit_sec=1.1):

    it = (
        df.select("country", "city", "lat", "lng")
          .dropna(subset=["country", "city"])
          .distinct()
          .toLocalIterator()
    )

    mapping = {}
    last_call = 0.0

    for row in it:
        ctr, city, lat, lng = row["country"], row["city"], row["lat"], row["lng"]
        if not is_invalid_coord(lat, lng):
            continue

        key = (ctr, city)
        if key in mapping:
            continue

        # simple rate limiting
        now = time.time()
        wait = max(0.0, rate_limit_sec - (now - last_call))
        if wait:
            time.sleep(wait)

        query = f"{city}, {ctr}"
        try:
            coords = opencage_geocode(query=query, api_key=api_key, country_code=country_code)
            if coords:
                mapping[key] = coords
        except Exception:
            pass
        finally:
            last_call = time.time()

    return mapping
```

3) **Weather deduplication before join (src/etl/enrich.py)**

    Problem: weather dataset can contain multiple rows per (geohash4, year, month)
(e.g. daily readings). If we join directly, restaurants can multiply.

    Solution: aggregate weather to month level per geohash4.
```python
def dedup_weather_for_join(weather_df):

    w = weather_df.filter(F.col("geohash4").isNotNull())

    # choose a "day id" column for counting days
    day_id_col = "wthr_date" if "wthr_date" in w.columns else ("day" if "day" in w.columns else None)

    aggs = [
        F.avg("avg_tmpr_c").alias("weather_avg_tmpr_c_month"),
        F.avg("avg_tmpr_f").alias("weather_avg_tmpr_f_month"),
        F.min("avg_tmpr_c").alias("weather_min_tmpr_c_month"),
        F.max("avg_tmpr_c").alias("weather_max_tmpr_c_month"),
    ]
    if day_id_col:
        aggs.append(F.countDistinct(day_id_col).alias("weather_days_cnt"))

    return w.groupBy("year", "month", "geohash4").agg(*aggs)
```
Output columns:
- weather_avg_tmpr_c_month, weather_avg_tmpr_f_month
- weather_min_tmpr_c_month, weather_max_tmpr_c_month
- weather_days_cnt

4) **Restaurants × Weather join (src/etl/enrich.py + job.py)**

    Weather data is already aggregated to monthly level by
dedup_weather_for_join(), so each (year, month, geohash4) appears only once.

    In job.py we create one row per restaurant per available month:
    ```python
    ym = weather_all.select("year", "month").distinct()
    restaurants_expanded = restaurants.crossJoin(ym)
    final_df = join_restaurants_weather(restaurants_expanded, weather_all)
   ```
   The join logic itself is implemented in join_restaurants_weather() (src/etl/enrich.py):
    ```python
    def join_restaurants_weather(restaurants_df, weather_df):
        # Idempotent left join: restaurants are preserved and not multiplied.
        weather_df = weather_df.dropDuplicates(["year", "month", "geohash4"])

        joined = restaurants_df.join(
            weather_df,
            on=["year", "month", "geohash4"],
            how="left"
        )

        # safety: protect against unexpected duplication
        if "id" in joined.columns:
            joined = joined.dropDuplicates(["id", "year", "month"])

        return joined
    ```
  Why this is idempotent and avoids multiplication:
- weather is unique by (year, month, geohash4) after aggregation
- we still enforce uniqueness as a safety guard with dropDuplicates
- left join preserves all restaurant-month rows
5) **Output format (src/etl/job.py)**

   Output is written as partitioned parquet: out/enriched/year=YYYY/month=MM/country=XX/
    ```python
    # 10) Write partitioned parquet (idempotent overwrite)
    (
        final_df.write
        .mode("overwrite")
        .partitionBy("year", "month", "country")
        .parquet(os.path.join(args.out, "enriched"))
    )
    ```
   Partitioning by (year, month, country) makes downstream analytics efficient
and scalable.
---
## Requirements

- Python 3.10+
- Apache Spark 3.5+
- Java 11+
- OpenCage API key

Install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
---

## OpenCage API Key

The pipeline requires an OpenCage API key to resolve missing coordinates.

Register at: https://opencagedata.com/

Copy your API key

Export it as environment variable:
```bash
export OPENCAGE_API_KEY="your_api_key_here"
```
---

## Package ETL code

From project root:
```bash
cd src
zip -r ../etl.zip etl
cd ..
```
## Running the Spark Job
```bash
spark-submit \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.default.parallelism=2 \
  --py-files etl.zip \
  src/etl/job.py \
  --restaurants "data/restaurants/part-*.csv" \
  --weather 2016-10=data/weather/2016-10 \
  --weather 2017-08=data/weather/2017-08 \
  --weather 2017-09=data/weather/2017-09 \
  --out out \
  --opencage-key "$OPENCAGE_API_KEY"
```