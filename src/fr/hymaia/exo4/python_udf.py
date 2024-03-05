import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
import time

# Create SparkSession
spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

# Define Python UDF
def category_name(category):
    if int(category) < 6:
        return 'food'
    else:
        return 'furniture'

# Register UDF
category_name_udf = udf(category_name, StringType())
start_time = time.time()

def addCategoryName(df):
    df = df.withColumn('category', df['category'].cast(IntegerType()))
    df = df.withColumn('price', df['price'].cast(DoubleType()))

    df = df.withColumn('category_name', category_name_udf(df['category']))

    return df

df = spark.read.csv('./src/resources/exo4/sell.csv', header=True)

df = addCategoryName(df)

# Persiste après l'application de la fonction d'UDF
df.persist()

# Filtrage
df_filtered = df.filter(df['category'] > 5)

# Transformation de colonnes
df_transformed = df_filtered.withColumn('category', df_filtered['category'].cast(IntegerType()))
df_transformed = df_transformed.withColumn('price', df_transformed['price'].cast(DoubleType()))

# Agrégation
df_aggregated = df_transformed.groupBy('category').agg(f.count('*').alias('count'))

# Tri
df_sorted = df_aggregated.orderBy('count', ascending=False)

# Affichage du résultat
df_sorted.show()

# Mesure du temps d'exécution
end_time = time.time()
execution_time = end_time - start_time
print("Temps d'exécution :", execution_time, "secondes")
