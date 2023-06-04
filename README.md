# **PySpark Project for Taxi Operation**


###  This project addresses the following topics:
* Load taxi dataset.
* Convert date columns to "YYYY-MM-DD".
* Data Cleaning and Preparation for taxi data.
* Load weather dataset.
* Join both datasets for further analysis.


### Project Structure

configs(dir)  - > contains config file for the application.\
schemas(dir)  - > contains schema for the datasets.\
tests(dir)  - > contains unittest classes and testsuite.\
utils(dir)  - > contains utilities classes and udf.

taxi_handler.py  - > Main handler to run the code.\
data_loader.py  - > Class containing methods to read dataset.\
transform_data.py  - > Class containing methods for transformations.