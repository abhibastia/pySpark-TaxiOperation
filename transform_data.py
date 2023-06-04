from pyspark.sql.functions import col, count, weekofyear
from pyspark.sql.functions import dayofweek, month, year, date_format, when, hour


class TransformData:
    def __init__(self, spark):
        self.spark = spark

    """
    Function to check null values of columns in a PySpark DataFrame.
    
    Parameters:
        - df (DataFrame): The DataFrame to perform null check.
    Returns:
        - df: The DataFrame containing null counts of columns 
    """

    def check_null_values(self, df):
        df = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        return df

    """
    Function to handle missing value for multiple columns in a PySpark DataFrame.
    
    Parameters:
        - df (DataFrame): The DataFrame to handle missing values
        - fill_values: dict containing column name and value to be filled
    Returns:
        - df: The DataFrame after filling null values 
    """

    def handle_missing_values(self, df, fill_values):
        for column, fill_value in fill_values.items():
            df = df.fillna(fill_value, subset=column)
        return df

    """
    Function to rename columns from a PySpark DataFrame.
    
    Parameters:
        - df (DataFrame): The DataFrame to perform renaming of columns
        - column_mapping: dict containing old_name and new_name to be changed
    Returns:
        - df: The DataFrame after renaming the column 
    """

    def rename_columns(self, df, column_mapping):
        for old_name, new_name in column_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    """
    Checks if all values in a specific column of a PySpark dataframe are unique.
    
    Parameters:
        - df (DataFrame): The DataFrame to uniqueness check
        - column_name: column to be check for uniqueness
    Returns:
        - Boolean: True if column is unique otherwise false
    """

    def check_uniqueness(self, df, column_name):
        return df.select(column_name).distinct().count() == df.count()

    """
    Checks if a specific column of a PySpark dataframe has the expected data type.
    
    Parameters:
        - df (DataFrame): The DataFrame to check data type on
        - column_name: column to be check for data type
        - expected_type: data type to be matched with
    Returns:
        - Boolean: True if data type matches otherwise false
    """

    def check_data_types(self, df, column_name, expected_type):
        return df.schema[column_name].dataType == expected_type

    """
    Checks if all values in a specific column of a PySpark dataframe are within a specified range.
    Parameters:
        - df (DataFrame): The DataFrame to perform data check.
        - column_name: Column name to check on
        - min_val: Min value
        - max_val: Max value
    Returns:
    - Boolean : True if falls within the range otherwise False
    """

    def check_value_range(self, df, column_name, min_val, max_val):
        return df.filter((col(column_name) < min_val) | (col(column_name) > max_val)).count() == 0

    """
    Perform data check to get null counts for all columns,no of duplicates in records,
    outlier counts, invalid_values, negative_values for specific columns
    
    Parameters:
        - df (DataFrame): The DataFrame to perform data check.
    Returns:
        - dict: The dict containing all 
    """

    def perform_data_checks(self, df):
        # Create an empty dictionary to store the results of the data checks.
        results = {}

        # Check for null counts in each column.
        null_counts = {column: df.filter(col(column).isNull()).count() for column in df.columns}
        results["null_counts"] = null_counts

        # Check for outliers.
        outlier_counts = {
            "passenger_count": df.filter((col("passenger_count") < 1) | (col("passenger_count") > 9)).count(),
            "trip_distance": df.filter(col("trip_distance") < 0.1).count(),
            "fare_amount": df.filter((col("fare_amount") < 2.5) | (col("fare_amount") > 200)).count(),
            "tolls_amount": df.filter(col("tolls_amount") < 0).count()
        }
        results["outlier_counts"] = outlier_counts

        # Check for duplicates.
        num_duplicates = df.count() - df.dropDuplicates().count()
        results["num_duplicates"] = num_duplicates

        # Check for invalid values in categorical columns.
        invalid_values = {
            "payment_type": df.filter(col("payment_type").isin(["1", "2", "3", "4", "5", "6"]) == False).count(),
            "RatecodeID": df.filter(col("RatecodeID").isin([1, 2, 3, 4, 5, 6]) == False).count(),
            "store_and_fwd_flag": df.filter(col("store_and_fwd_flag").isin(["Y", "N"]) == False).count()
        }
        results["invalid_values"] = invalid_values

        # Check for negative values in important columns
        negative_values = {
            "passenger_count": df.filter(col("passenger_count") < 0).count(),
            "trip_distance": df.filter(col("trip_distance") < 0).count(),
            "fare_amount": df.filter(col("fare_amount") < 0).count()
        }
        results["negative_values"] = negative_values

        return results

    """
        Derive columns like day, month, year, etc.. from a date column
        
        Parameters:
            - df (DataFrame): The DataFrame to derive from.
            - date_column : Date column from which columns to be derived.
        
        Returns:
        - DataFrame: The DataFrame with derived columns.
    """

    def derive_date_columns(self, df, date_column):
        return df.withColumn("day", dayofweek(col(date_column))) \
            .withColumn("month", month(col(date_column))) \
            .withColumn("year", year(col(date_column))) \
            .withColumn("day_of_week", date_format(col(date_column), "EEEE")) \
            .withColumn("weekend", when(col("day_of_week").isin(["Saturday", "Sunday"]), 1).otherwise(0)) \
            .withColumn("hour", hour(col(date_column))) \
            .withColumn("week_of_year", weekofyear(col(date_column)))
