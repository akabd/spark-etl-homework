from etl.enrich import dedup_weather_for_join, join_restaurants_weather


def test_join_no_multiplication(spark):
    restaurants = spark.createDataFrame(
        [(1, 2016, 10, "abcd"), (2, 2016, 10, "abcd"), (3, 2016, 10, "zzzz")],
        ["id", "year", "month", "geohash4"]
    )

    weather = spark.createDataFrame(
        [
            ("abcd", 2016, 10, 10.0, 50.0, "2016-10-01", 1),
            ("abcd", 2016, 10, 12.0, 55.0, "2016-10-02", 2),
            ("zzzz", 2016, 10, 5.0, 41.0, "2016-10-01", 1),
        ],
        ["geohash4", "year", "month", "avg_tmpr_c", "avg_tmpr_f", "wthr_date", "day"],
    )

    weather_d = dedup_weather_for_join(weather)
    joined = join_restaurants_weather(restaurants, weather_d)

    assert joined.count() == restaurants.count()
