import logging

from dateutil.parser import parse

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

"""
Convert a date string to the "YYYY-MM-DD" format.

Parameters:
- date_str (str): The input date string.

Returns:
- str: The converted date string in the "YYYY-MM-DD" format.
"""


def convert_to_date(date_str):
    try:
        parsed_date = parse(date_str)
        return parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        logging.error(f"Exception in applying udf")
        return date_str


# Register the UDF
convert_to_date_udf = udf(convert_to_date, StringType())
