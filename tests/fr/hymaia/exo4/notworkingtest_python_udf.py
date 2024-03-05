import unittest
from pyspark.sql import Row
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.python_udf import addCategoryName

class TestPythonUDF(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    def test_addCategoryName(self):
        # Create a DataFrame for testing
        test_data = [Row(id=1, date="2019-02-17", category=6, price=40.0),
                     Row(id=2, date="2019-02-18", category=5, price=50.0)]
        df = self.spark.createDataFrame(test_data)

        # Apply the addCategoryName function
        df = addCategoryName(df)

        # Collect the result to the driver
        result = df.collect()

        # Check the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["category_name"], "furniture")
        self.assertEqual(result[1]["category_name"], "food")

    def tearDown(self):
        self.spark.stop()

if __name__ == "__main__":
    unittest.main()