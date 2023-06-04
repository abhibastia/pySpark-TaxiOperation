import argparse

from data_loader import DataLoader
from py4j.clientserver import logger
from pyspark.sql.functions import col, month
from schemas.schema import taxi_schema, weather_schema
from transform_data import TransformData
from utils.spark_utils import SparkUtils
from utils.udf_utils import convert_to_date_udf


class TaxiHandler:
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.spark_util = None
        self.data_loader = None
        self.transform_data = None

    def load_config(self):
        self.spark_util = SparkUtils()
        self.spark_util.initialize(self.config_file_path)

    def run(self):
        try:
            # Initialize the initial configuration
            self.load_config()
            logger.info("Initial configurations are initialized")

            # Get Spark Session
            spark = self.spark_util.get_spark_session()

            # Get properties from config file
            properties = self.spark_util.get_properties(self.config_file_path)
            taxi_dataset_path = properties["Data"]["taxi_dataset_path"]
            weather_dataset_path = properties["Data"]["weather_dataset_path"]
            taxi_file_format = properties["Data"]["taxi_file_format"]
            weather_file_format = properties["Data"]["weather_file_format"]
            log_level = properties['Application']['log_level']

            # Set the log level for Spark
            spark.sparkContext.setLogLevel(log_level.upper())

            # Create an instance of DataLoader
            self.data_loader = DataLoader(spark)

            # Load taxi dataset
            taxi_df = self.data_loader.read_data(taxi_file_format, taxi_dataset_path, taxi_schema)
            logger.info("Taxi data is loaded")

            # Apply UDF to convert date format in taxi trip data
            taxi_date_df = taxi_df.withColumn("pickup_date", convert_to_date_udf(col("tpep_pickup_datetime"))) \
                .withColumn("dropoff_date", convert_to_date_udf(col("tpep_dropoff_datetime")))
            logger.info("Date columns are converted to date type using udf")

            # Create an instance of TransformData
            self.transform_data = TransformData(spark)

            # Perform data check for taxi dataset
            result_data_check = self.transform_data.perform_data_checks(taxi_date_df)
            logger.warn(f"Result of data check for taxi data : {result_data_check}")

            # Remove duplicate from taxi dataset
            taxi_dedup_df = taxi_date_df.dropDuplicates()

            # Define columns to fill missing values and the fill value
            columns_to_fill = ["congestion_surcharge"]
            fill_value = 0.0
            taxi_fill_missing_df = taxi_dedup_df.fillna(fill_value, columns_to_fill)

            # Define condition for filtering records based on taxi specific data
            condition = ((col("total_amount") < 10000) &
                         (col("total_amount") > 0) &
                         (col("trip_distance") > 0) &
                         (col("trip_distance") < 150) &
                         (col("passenger_count") < 10) &
                         (col("pickup_date") >= "2019-01-01") &
                         (col("pickup_date") < "2019-01-31"))
            # Filter records based on the condition
            taxi_filtered_df = taxi_fill_missing_df.filter(condition)
            logger.info("Taxi data is filtered based on conditions")

            # Derive date related columns
            taxi_trip_df = self.transform_data.derive_date_columns(taxi_filtered_df, "tpep_pickup_datetime")
            logger.info("Date columns such as day,month,week etc. are derived for taxi data")

            # Load weather dataset
            weather_df = self.data_loader.read_data(weather_file_format, weather_dataset_path, weather_schema)
            logger.info("Weather data is loaded")

            # Define column mapping for renaming
            column_map = {"date": "weather_date"}
            # Rename the columns
            weather_df = self.transform_data.rename_columns(weather_df, column_map)

            # Filter weather data for January
            weather_filtered_df = weather_df.filter(month(col("weather_date")) == 1)

            # Join taxi trips data and weather datasets
            join_expr = col("pickup_date") == col("weather_date")
            joined_df = taxi_trip_df.join(weather_filtered_df, join_expr, "left")
            joined_df.show(5, False)
            logger.info("Taxi and weather data are joined")

        except Exception as e:
            logger.error("Error in handler: %s", str(e), exc_info=True)


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Taxi Operation")
    parser.add_argument("--config", dest="config_file", required=True, help="Path to config file")
    args = parser.parse_args()
    config_file = args.config_file

    # Create an instance of TaxiHandler
    app = TaxiHandler(config_file)
    app.run()
