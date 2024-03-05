from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum
import time
from pyspark.sql.window import Window

def addCategoryName(col):
    return when(col < 6, "food").otherwise("furniture")
    

def main():
    spark = SparkSession.builder \
        .appName("NoUDF") \
        .master("local[*]") \
        .getOrCreate()

    start_time = time.time()

    data_path = "src/resources/exo4/sell.csv"
    df = spark.read.option("header", "true").csv(data_path)
    
    # Transformation de colonnes
    df = df.withColumn("price", col("price").cast("int"))
    df = df.withColumn("category", col("category").cast("int"))

    # Ajout de la nouvelle colonne avec des conditions
    df = df.withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))

    # Persiste après la première transformation
    df.persist()

    # Définition de la fenêtre pour la somme des prix par catégorie et par jour
    window = Window.partitionBy('category_name', 'date')
    df = df.withColumn('total_price_per_category_per_day', sum('price').over(window))

    # Définition de la fenêtre glissante de 30 jours pour la somme des prix par catégorie
    window_30_days = Window.partitionBy('category_name', 'date').rowsBetween(-30, 0)
    df = df.withColumn('total_price_per_category_per_day_last_30_days', sum('price').over(window_30_days))

    # Filtrage des lignes où 'category' est supérieur à 5
    df = df.filter(df['category'] > 5)

    # Groupement par 'category_name' et comptage
    df = df.groupBy('category_name').count()

    end_time = time.time()
    execution_time = end_time - start_time
    print("Temps d'exécution no_udf avec persiste après la première transformation:", execution_time, "secondes")

if __name__ == "__main__":
    main()
