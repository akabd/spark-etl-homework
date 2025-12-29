from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType
)


def read_restaurants(spark: SparkSession, path_glob: str):
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("franchise_id", IntegerType(), True),
        StructField("franchise_name", StringType(), True),
        StructField("restaurant_franchise_id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
    ])

    df = spark.read.option("header", "true").schema(schema).csv(path_glob)

    return (
        df.withColumn("country", F.upper(F.trim(F.col("country"))))
          .withColumn("city", F.initcap(F.trim(F.col("city"))))
          .withColumn("franchise_name", F.trim(F.col("franchise_name")))
    )


def read_weather_month(spark: SparkSession, path: str, year: int, month: int):
    """
    Читаем parquet папку и добавляем year/month как партиционные колонки.
    """
    return (
        spark.read.parquet(path)
        .withColumn("year", F.lit(int(year)))
        .withColumn("month", F.lit(int(month)))
    )
