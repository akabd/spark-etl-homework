import os
import argparse
from pyspark.sql import SparkSession, functions as F

from etl.io import read_restaurants, read_weather_month
from etl.geohash_udf import geohash4_udf
from etl.geocode import build_missing_coords_mapping
from etl.enrich import (
    enrich_restaurants_with_mapping,
    dedup_weather_for_join,
    join_restaurants_weather
)


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_weather_args(weather_args: list[str]):
    """
    Принимаем аргументы вида:
      --weather 2016-10=/path/to/weather/2016-10
      --weather 2017-08=/path/to/weather/2017-08
      --weather 2017-09=/path/to/weather/2017-09
    """
    parsed = []
    for item in weather_args:
        # "2016-10=/path"
        ym, path = item.split("=", 1)
        year_s, month_s = ym.split("-", 1)
        parsed.append((int(year_s), int(month_s), path))
    return parsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restaurants", required=True, help="Restaurants CSV glob, e.g. data/restaurants/part-*.csv")
    parser.add_argument("--weather", action="append", required=True,
                        help="Weather month mapping: YYYY-MM=/path/to/parquet_folder (repeatable)")
    parser.add_argument("--out", required=True, help="Output base folder for parquet")
    parser.add_argument("--opencage-key", default=os.getenv("OPENCAGE_API_KEY"), help="OpenCage API key")
    parser.add_argument("--opencage-country-code", default=None, help="Optional country bias, e.g. it")
    args = parser.parse_args()

    if not args.opencage_key:
        raise SystemExit("Missing OpenCage API key. Provide --opencage-key or set OPENCAGE_API_KEY.")

    spark = build_spark("restaurants-weather-etl")

    # 1) Extract restaurants
    restaurants = read_restaurants(spark, args.restaurants)

    # 2) Add temporary geohash
    restaurants = restaurants.withColumn("geohash4", geohash4_udf(F.col("lat"), F.col("lng")))

    # 3) Build mapping for missing coords (distinct city+country)
    mapping = build_missing_coords_mapping(
        df=restaurants,
        api_key=args.opencage_key,
        country_code=args.opencage_country_code
    )

    # 4) Fill coords using mapping
    restaurants = enrich_restaurants_with_mapping(restaurants, mapping)

    # 5) Recompute geohash4 after fixing coords
    restaurants = restaurants.withColumn("geohash4", geohash4_udf(F.col("lat"), F.col("lng")))

    # 6) Read + union weather months
    months = parse_weather_args(args.weather)

    weather_all = None
    for (y, m, path) in months:
        w = read_weather_month(spark, path, y, m)

        # ensure geohash4 exists (either already or computed from lat/lng)
        if "geohash4" not in w.columns:
            if "lat" in w.columns and "lng" in w.columns:
                w = w.withColumn("geohash4", geohash4_udf(F.col("lat"), F.col("lng")))
            else:
                raise SystemExit(f"Weather dataset at {path} has no geohash4 and no lat/lng to compute it.")

        weather_all = w if weather_all is None else weather_all.unionByName(w, allowMissingColumns=True)

    # 7) Dedup weather to avoid join multiplication
    weather_all = dedup_weather_for_join(weather_all)

    # 8) Create (year, month) table and expand restaurants to each month
    ym = weather_all.select("year", "month").distinct()
    restaurants_expanded = restaurants.crossJoin(ym)

    # 9) Left join
    final_df = join_restaurants_weather(restaurants_expanded, weather_all)

    # 10) Write partitioned parquet (idempotent overwrite)
    (
        final_df.write
        .mode("overwrite")
        .partitionBy("year", "month", "country")
        .parquet(os.path.join(args.out, "enriched"))
    )

    spark.stop()


if __name__ == "__main__":
    main()
