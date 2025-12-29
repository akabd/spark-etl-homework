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