import unittest
from src.fr.hymaia.exo4.no_udf import main
from tests.fr.hymaia.spark_test_case import spark

class TestNoUDF(unittest.TestCase):
    def test_main(self):
        # Prepare the DataFrame
        df = spark.createDataFrame([(1, "2019-02-17", 6, 40.0)], ["id", "date", "category", "price"])
        
        # Replace the `spark.read.csv` call with a function that returns the prepared DataFrame
        spark.read.csv = lambda *args, **kwargs: df

        # Call the main function
        try:
            main()
            self.assertTrue(True)  # If no exception is thrown, the test passes
        except Exception as e:
            self.fail(f"main raised an exception: {e}")

if __name__ == "__main__":
    unittest.main()