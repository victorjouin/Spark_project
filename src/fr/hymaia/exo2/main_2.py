from pyspark.sql import SparkSession
from function import filter_majeur, join_villes, add_departement, pop_depart, write_csv

def main():
    spark = SparkSession.builder.master("local[*]").appName("exo2part2").getOrCreate()
    clean = spark.read.parquet("data/exo2/clean")
    clean.show(truncate=False)
    pop_depart_df = pop_depart(clean)
    pop_depart_df.show(truncate=False)
    write_csv(pop_depart_df,  "data/exo2/aggregate")
    # afficher le chemin du CSV créé
    print("CSV written to: data/exo2/aggregate")

    spark.stop()

if __name__ == "__main__":
    main()