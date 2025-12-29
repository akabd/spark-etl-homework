from pyspark.sql import functions as F


def enrich_restaurants_with_mapping(restaurants_df, mapping: dict):

    sc = restaurants_df.sparkSession.sparkContext
    bmap = sc.broadcast(mapping)

    def pick_lat(country, city, lat, lng):
        try:
            if lat is not None and lng is not None:
                latf = float(lat); lngf = float(lng)
                if -90 <= latf <= 90 and -180 <= lngf <= 180:
                    return latf
        except Exception:
            pass
        v = bmap.value.get((country, city))
        return v[0] if v else None

    def pick_lng(country, city, lat, lng):
        try:
            if lat is not None and lng is not None:
                latf = float(lat); lngf = float(lng)
                if -90 <= latf <= 90 and -180 <= lngf <= 180:
                    return lngf
        except Exception:
            pass
        v = bmap.value.get((country, city))
        return v[1] if v else None

    pick_lat_udf = F.udf(pick_lat, "double")
    pick_lng_udf = F.udf(pick_lng, "double")

    return (
        restaurants_df
        .withColumn("lat", pick_lat_udf(F.col("country"), F.col("city"), F.col("lat"), F.col("lng")))
        .withColumn("lng", pick_lng_udf(F.col("country"), F.col("city"), F.col("lat"), F.col("lng")))
    )


def _find_time_col(df):
    for c in ["timestamp", "dt", "time", "date_time", "event_time", "datetime", "date"]:
        if c in df.columns:
            return c
    return None


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


def join_restaurants_weather(restaurants_df, weather_df):
    """
    Idempotent left join: рестораны остаются как есть, не умножаются.
    """
    weather_df = weather_df.dropDuplicates(["year", "month", "geohash4"])

    joined = restaurants_df.join(
        weather_df,
        on=["year", "month", "geohash4"],
        how="left"
    )

    # safety: если вдруг появились дубли — фиксируем по id+month
    if "id" in joined.columns:
        joined = joined.dropDuplicates(["id", "year", "month"])

    return joined
