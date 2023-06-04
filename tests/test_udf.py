import logging
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.udf_utils import convert_to_date_udf


class UDFTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder.master("local") \
            .appName("UDFTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession after testing
        cls.spark.stop()

    def test_convert_to_date(self):
        # Define test data
        input_data = [("2023-05-01", "2023-05-01"),
                      ("May 1, 2023", "2023-05-01"),
                      ("01/02/2023", "2023-01-02"),
                      ("2/1/19", "2019-02-01"),
                      ("2019-01-01T00:46:40.000+0000", "2019-01-01")]

        # Create a DataFrame from the test data
        df = self.spark.createDataFrame(input_data, ["input_date", "expected_output"])

        # Apply the UDF to the input_date column
        df = df.withColumn("converted_date", convert_to_date_udf(col("input_date")))

        # Collect the results as a list of rows
        result = df.collect()

        # Iterate through the result rows and perform assertions
        for row in result:
            expected_output = row["expected_output"]
            converted_date = row["converted_date"]

            # Check if the converted date matches the expected output
            self.assertEqual(converted_date, expected_output)
        logging.info("Test case 'test_convert_to_date' passed successfully!")


if __name__ == '__main__':
    unittest.main()
