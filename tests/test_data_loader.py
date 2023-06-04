import logging
import unittest

from data_loader import DataLoader
from pyspark.sql import SparkSession
from schemas.schema import weather_schema


class DataLoaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("DataLoaderTest").getOrCreate()
        cls.data_loader = DataLoader(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # unit test for read_data() with valid file
    def test_read_data_with_valid_file(self):
        file_format = 'csv'
        file_path = 'data\\nyc_weather.csv'
        expected_df_count = 365
        df = self.data_loader.read_data(file_format, file_path, weather_schema)

        self.assertIsNotNone(df)
        self.assertEqual(df.count(), expected_df_count)
        logging.info("Test case 'test_read_data_with_valid_file' passed successfully!")

    # unit test for read_data() with invalid file
    def test_read_data_with_invalid_file(self):
        file_format = 'docx'
        file_path = 'data\\hello.docx'
        df = self.data_loader.read_data(file_format, file_path, weather_schema)

        self.assertIsNone(df)
        self.assertIn(file_path, self.data_loader.get_corrupted_files())
        logging.info("Test case 'test_read_data_with_invalid_file' passed successfully!")


if __name__ == '__main__':
    unittest.main()
