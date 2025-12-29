import pygeohash as pgh
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _geohash4(lat, lng):
    if lat is None or lng is None:
        return None
    try:
        return pgh.encode(float(lat), float(lng), precision=4)
    except Exception:
        return None


geohash4_udf = F.udf(_geohash4, StringType())
