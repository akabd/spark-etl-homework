from pyspark.sql import functions as F
from etl.geohash_udf import geohash4_udf


def test_geohash4_udf(spark):
    df = spark.createDataFrame([(45.4642, 9.1900)], ["lat", "lng"])
    out = df.withColumn("gh", geohash4_udf(F.col("lat"), F.col("lng"))).collect()[0]["gh"]
    assert out is not None
    assert len(out) == 4
