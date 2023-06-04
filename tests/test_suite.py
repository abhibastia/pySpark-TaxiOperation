import unittest

from tests.test_data_loader import DataLoaderTest
from tests.test_transform_data import TransformDataTest
from tests.test_udf import UDFTest

# Create a test suite
test_suite = unittest.TestSuite()

# Add test cases to the test suite
test_suite.addTest(unittest.makeSuite(DataLoaderTest))
test_suite.addTest(unittest.makeSuite(TransformDataTest))
test_suite.addTest(unittest.makeSuite(UDFTest))

# Run the test suite
unittest.TextTestRunner().run(test_suite)