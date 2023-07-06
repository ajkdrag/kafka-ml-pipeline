import pyspark.sql.types as T
import pyspark.sql.functions as F
from math import radians, sin, atan2, cos, sqrt


@F.udf(returnType=T.DoubleType())
def get_haversine_distance_udf(lat1, lon1, lat2, lon2):
    R = 6371
    dlon = radians(lon2) - radians(lon1)
    dlat = radians(lat2) - radians(lat1)

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c
