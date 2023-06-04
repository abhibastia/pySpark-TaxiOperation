from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Define the schema for NYC Yellow Taxi data
taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# Define the schema for weather data
weather_schema = StructType([
    StructField("date", StringType(), True),
    StructField("tmax", IntegerType(), True),
    StructField("tmin", IntegerType(), True),
    StructField("tavg", DoubleType(), True),
    StructField("departure", DoubleType(), True),
    StructField("HDD", IntegerType(), True),
    StructField("CDD", IntegerType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("new_snow", DoubleType(), True),
    StructField("snow_depth", DoubleType(), True),
])
