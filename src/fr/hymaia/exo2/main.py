import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.function import filter_majeur
from src.fr.hymaia.exo2.function import join_villes
from src.fr.hymaia.exo2.function import add_departement

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Clean Job") \
        .master("local[*]") \
        .getOrCreate()
    clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
    filtered_clients_df = filter_majeur(clients_df)
    villes_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)
    joined_df = join_villes(filtered_clients_df, villes_df)
    final_df = add_departement(joined_df)
    final_df.write.parquet("data/exo2/clean", mode="overwrite")

if __name__ == "__main__":
    main()

    
