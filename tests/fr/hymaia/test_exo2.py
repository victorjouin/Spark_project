import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.function import filter_majeur, join_villes, add_departement, pop_depart
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql.utils import AnalysisException
class TestSparkFunctions(unittest.TestCase):

        
    def test_filter_majeur(self):
        # Given
        data = [Row(age=20), Row(age=17)]
        df = spark.createDataFrame(data)
        expected_count = 1
        # When
        result = filter_majeur(df)
        # Then
        self.assertEqual(result.count(), expected_count)

    def test_join_villes(self):
        # Given
        clients_data = [Row(zip=123), Row(zip=456)]
        villes_data = [Row(zaip=123, city="Paris"), Row(zaip=456, city="Marseille")]
        clients_df = spark.createDataFrame(clients_data)
        villes_df = spark.createDataFrame(villes_data)
        expected_count = 2
        # When
        result = join_villes(clients_df, villes_df)
        # Then
        self.assertEqual(result.count(), expected_count)

    def test_add_departement(self):
        # Given
        data = [Row(zip='20190'), Row(zip='20200'), Row(zip='75000')]
        df = spark.createDataFrame(data)
        expected_data = [('20190', '2A'), ('20200', '2B'), ('75000', '75')]
        expected_df = spark.createDataFrame(expected_data, ["zip", "departement"])
        # When
        result = add_departement(df)
        # Then
        self.assertEqual(result.collect(), expected_df.collect())

    def test_pop_depart(self):
        # Given
        data = [Row(departement='2A'), Row(departement='2B'), Row(departement='75'),Row(departement='2B')]
        df = spark.createDataFrame(data)
        expected_count = 3  
        # When
        result = pop_depart(df)
        # Then
        self.assertEqual(result.count(), expected_count)




# ERROR PART

    def test_filter_majeur_error(self):
        # Given
        data = [Row(age='twenty'), Row(age='seventeen')]
        df = spark.createDataFrame(data)
        # When & Then
        with self.assertRaises(TypeError):
            filter_majeur(df)

    def test_join_villes_error(self):
        # Given
        clients_data = [Row(zip=123), Row(zip=456)]
        villes_data = [Row(zipcode=123, city="Paris"), Row(zipcode=456, city="Marseille")]
        clients_df = spark.createDataFrame(clients_data)
        villes_df = spark.createDataFrame(villes_data)
        # When & Then
        with self.assertRaises(AnalysisException):
            join_villes(clients_df, villes_df)

    def test_add_departement_error(self):
        # Given
        data = [Row(zipcode='20190'), Row(zipcode='20200'), Row(zipcode='05000')]
        df = spark.createDataFrame(data)
        # When & Then
        with self.assertRaises(AnalysisException):
            add_departement(df)

    def test_pop_depart_error(self):
        # Given
        data = [Row(department='2A'), Row(department='2B'), Row(department='05'),Row(department='2B')]
        df = spark.createDataFrame(data)
        # When & Then
        with self.assertRaises(TypeError):
            pop_depart(df)


# INTEGRATION TEST

    def test_integration(self):
        # Given
        clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
        villes_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)
        expected_count = 129352

        # When
        filtered_clients_df = filter_majeur(clients_df)
        joined_df = join_villes(filtered_clients_df, villes_df)
        final_df = add_departement(joined_df)
        pop_depart_df = pop_depart(final_df)
        final_df.write.parquet("data/exo2/output_test", mode="overwrite")
        result_df = spark.read.parquet("data/exo2/output_test")

        # Then
        self.assertEqual(filtered_clients_df.count(), 9971)
        self.assertEqual(joined_df.count(), 129352)
        self.assertEqual(final_df.count(), 129352)
        self.assertEqual(result_df.count(), expected_count)
        self.assertEqual(pop_depart_df.count(), 90)

        # assertions  number of columns
        self.assertEqual(len(filtered_clients_df.columns), 3)  
        self.assertEqual(len(joined_df.columns), 4)  
        self.assertEqual(len(final_df.columns), 5) 
        self.assertEqual(len(result_df.columns), 5)
        self.assertEqual(len(pop_depart_df.columns), 2)

        # assertions column names
        self.assertEqual(filtered_clients_df.columns, ['name', 'age', 'zip'])  
        self.assertEqual(joined_df.columns, ['zip', 'name', 'age', 'city'])  
        self.assertEqual(final_df.columns, ['zip', 'name', 'age', 'city', 'departement'])  
        self.assertEqual(result_df.columns, ['zip', 'name', 'age', 'city', 'departement']) 
        self.assertEqual(pop_depart_df.columns, ['departement', 'nb_people']) 
if __name__ == "__main__":
    unittest.main()