import unittest
from src.fr.hymaia.exo4.no_udf import main
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.no_udf import addCategoryName
from pyspark.sql import Row
from pyspark.sql.functions import col

class TestNoUDF(unittest.TestCase):

    def test_add_category_name(self):
        # Create a DataFrame for testing
        test_data = [Row(id=1, date="2019-02-17", category=6, price=40.0),
                     Row(id=2, date="2019-02-18", category=5, price=50.0)]
        df = spark.createDataFrame(test_data)

        # Apply the addCategoryName function to the 'category' column
        df = df.withColumn("category_name", addCategoryName(col("category")))

        # Collect the result to the driver
        result = df.collect()

        # Check the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["category_name"], "furniture")
        self.assertEqual(result[1]["category_name"], "food")

    def tearDown(self):
        spark.stop()

if __name__ == "__main__":
    unittest.main()