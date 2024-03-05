import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
import time

spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()
def category_name(category):
    if int(category) < 6:
        return 'food'
    else:
        return 'furniture'
category_name_udf = udf(category_name, StringType())
start_time = time.time()

def addCategoryName(df):
    df = df.withColumn('category', df['category'].cast(IntegerType()))
    df = df.withColumn('price', df['price'].cast(DoubleType()))

    df = df.withColumn('category_name', category_name_udf(df['category']))

    return df

df = spark.read.csv('./src/resources/exo4/sell.csv', header=True)

df = addCategoryName(df)
df.persist()

df_filtered = df.filter(df['category'] > 5)

df_transformed = df_filtered.withColumn('category', df_filtered['category'].cast(IntegerType()))
df_transformed = df_transformed.withColumn('price', df_transformed['price'].cast(DoubleType()))

df_aggregated = df_transformed.groupBy('category').agg(f.count('*').alias('count'))

df_sorted = df_aggregated.orderBy('count', ascending=False)

end_time = time.time()
execution_time = end_time - start_time
print("Temps d'exécution :", execution_time, "secondes")
