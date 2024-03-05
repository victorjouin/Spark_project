import unittest
from pyspark.sql import Row
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.scala_udf import addCategoryName

class TestScalaUDF(unittest.TestCase):
    def test_addCategoryName(self):
        # Create a DataFrame for testing
        test_data = [Row(id=1, date="2019-02-17", category=6, price=40.0)]
        df = spark.createDataFrame(test_data)

        # Apply the addCategoryName function to the 'category' column
        df = df.withColumn("category_name", addCategoryName(df["category"]))

        # Collect the result to the driver
        result = df.collect()

        # Check the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["category_name"], "furniture")

if __name__ == "__main__":
    unittest.main()