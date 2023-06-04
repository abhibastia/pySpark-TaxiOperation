import logging
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, DoubleType
from transform_data import TransformData


class TransformDataTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for unit testing
        cls.spark = SparkSession.builder.master("local").appName("TransformDataTest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after unit testing
        cls.spark.stop()

    def setUp(self):
        # Initialize TransformData instance for each test case
        self.transformer = TransformData(self.spark)

    # Test check_null_values method
    def test_check_null_values(self):
        # Create a DataFrame for testing
        data = [(1, "John", 25), (2, "Jane", None), (3, None, 30)]
        df = self.spark.createDataFrame(data, ["id", "name", "age"])

        # Apply the check_null_values method
        result = self.transformer.check_null_values(df)
        row = result.collect()[0]
        actual_result = [row['id'], row['name'], row['name']]

        # Assert the expected result
        expected_result = [0, 1, 1]
        self.assertEqual(actual_result, expected_result)
        logging.info("Test case 'test_check_null_values' passed successfully!")

    # Test handle_missing_values method
    def test_handle_missing_values(self):
        # Create a sample DataFrame
        data = [("Alice", 25, None), ("Bob", None, "Male"), (None, 30, "Male")]
        columns = ["name", "age", "gender"]
        df = self.spark.createDataFrame(data, columns)

        # Define the fill values
        fill_values = {"name": "Unknown", "age": 0, "gender": "Unknown"}

        # Apply the handle_missing_values method
        actual_df = self.transformer.handle_missing_values(df, fill_values)

        # Check if missing values are filled correctly
        expected_data = [("Alice", 25, "Unknown"), ("Bob", 0, "Male"), ("Unknown", 30, "Male")]
        expected_df = self.spark.createDataFrame(expected_data, columns)

        self.assertEqual(actual_df.collect(), expected_df.collect())
        logging.info("Test case 'test_handle_missing_values' passed successfully!")

    # Test rename_columns method
    def test_rename_columns(self):
        # Create a sample DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        df = self.spark.createDataFrame(data, columns)

        # Define the column mapping for renaming
        column_mapping = {"name": "full_name", "age": "years"}

        # Rename columns using the transform_data method
        transformed_df = self.transformer.rename_columns(df, column_mapping)

        # Assert that the column names have been renamed correctly
        expected_columns = ["full_name", "years"]
        actual_columns = transformed_df.columns
        self.assertEqual(expected_columns, actual_columns)

        # Assert that the original DataFrame is not modified
        original_columns = df.columns
        self.assertEqual(columns, original_columns)
        logging.info("Test case 'test_rename_columns' passed successfully!")

    # Test check_uniqueness method
    def test_check_uniqueness(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [
            (1, "John", 25),
            (2, "Jane", 30),
            (3, "Mike", 35),
            (4, "John", 40)
        ]
        df = self.spark.createDataFrame(data, schema)

        # Assert the uniqueness of column
        self.assertFalse(self.transformer.check_uniqueness(df, "name"))
        self.assertTrue(self.transformer.check_uniqueness(df, "id"))
        logging.info("Test case 'test_check_uniqueness' passed successfully!")

    # Test check_data_types method
    def test_check_data_types(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", DoubleType(), True)
        ])
        data = [
            (1, "John", 25, 180.5),
            (2, "Jane", 30, 165.3),
            (3, "Mike", 35, 175.0)
        ]
        df = self.spark.createDataFrame(data, schema)

        self.assertTrue(self.transformer.check_data_types(df, "id", IntegerType()))
        self.assertTrue(self.transformer.check_data_types(df, "name", StringType()))
        self.assertTrue(self.transformer.check_data_types(df, "age", IntegerType()))
        self.assertTrue(self.transformer.check_data_types(df, "height", DoubleType()))

        self.assertFalse(self.transformer.check_data_types(df, "id", StringType()))
        self.assertFalse(self.transformer.check_data_types(df, "name", IntegerType()))
        self.assertFalse(self.transformer.check_data_types(df, "age", DoubleType()))
        self.assertFalse(self.transformer.check_data_types(df, "height", StringType()))
        logging.info("Test case 'test_check_data_types' passed successfully!")

    # Test check_value_range method
    def test_check_value_range(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [
            (1, "John", 25),
            (2, "Jane", 30),
            (3, "Mike", 35),
            (4, "John", 40)
        ]
        df = self.spark.createDataFrame(data, schema)

        self.assertTrue(self.transformer.check_value_range(df, "id", 1, 4))
        self.assertFalse(self.transformer.check_value_range(df, "id", 2, 3))
        self.assertTrue(self.transformer.check_value_range(df, "age", 25, 40))
        self.assertFalse(self.transformer.check_value_range(df, "age", 20, 30))
        logging.info("Test case 'test_check_value_range' passed successfully!")

    # Test perform_data_checks method
    def test_perform_data_checks(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("payment_type", StringType(), True),
            StructField("RatecodeID", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True)
        ])
        data = [
            (2, 2.5, 10.0, 1.0, "1", 1, "Y"),
            (0, -1.0, 1.0, 0.0, "2", 2, "N"),
            (4, 5.0, 300.0, -2.0, "3", 3, "Y"),
            (1, 0.05, 5.0, 0.0, "4", 4, "N"),
            (10, 100.0, 250.0, 1.5, "5", 5, "N"),
            (3, 4.5, 50.0, 0.0, "6", 6, "Y")
        ]
        df = self.spark.createDataFrame(data, schema)

        actual_results = self.transformer.perform_data_checks(df)
        expected_results = {
            "null_counts": {
                "passenger_count": 0,
                "trip_distance": 0,
                "fare_amount": 0,
                "tolls_amount": 0,
                "payment_type": 0,
                "RatecodeID": 0,
                "store_and_fwd_flag": 0
            },
            "outlier_counts": {
                "passenger_count": 2,
                "trip_distance": 2,
                "fare_amount": 3,
                "tolls_amount": 1
            },
            "num_duplicates": 0,
            "invalid_values": {
                "payment_type": 0,
                "RatecodeID": 0,
                "store_and_fwd_flag": 0
            },
            "negative_values": {
                "passenger_count": 0,
                "trip_distance": 1,
                "fare_amount": 0
            }
        }

        self.assertEqual(actual_results, expected_results)
        logging.info("Test case 'test_perform_data_checks' passed successfully!")

    # Test derive_date_columns method
    def test_derive_date_columns(self):
        # Create a sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", StringType(), True)
        ])
        data = [
            (1, "John", "2023-05-01"),
            (2, "Jane", "2023-05-02"),
            (3, "Mike", "2023-05-03")
        ]
        df = self.spark.createDataFrame(data, schema)

        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
            StructField("day", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("day_of_week", StringType(), True),
            StructField("weekend", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("week_of_year", IntegerType(), True)
        ])
        expected_data = [
            (1, "John", "2023-05-01", 2, 5, 2023, "Monday", 0, 0, 18),
            (2, "Jane", "2023-05-02", 3, 5, 2023, "Tuesday", 0, 0, 18),
            (3, "Mike", "2023-05-03", 4, 5, 2023, "Wednesday", 0, 0, 18)
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        actual_df = self.transformer.derive_date_columns(df, "date")

        self.assertListEqual(actual_df.collect(), expected_df.collect())
        logging.info("Test case 'test_derive_date_columns' passed successfully!")


if __name__ == '__main__':
    unittest.main()
