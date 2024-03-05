import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

spark = SparkSession.builder.appName("exo4_scala")\
        .master("local[*]")\
        .config('spark.jars', 'src/resources/exo4/udf.jar')\
        .getOrCreate()

start_time = time.time()

def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

dfSell = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")

dfTransformed = dfSell.withColumn("category", dfSell['category'].cast("int"))
dfTransformed = dfTransformed.withColumn("price", dfTransformed['price'].cast("double"))

dfTransformed.persist()

dfCateName = dfTransformed.withColumn("category_name", addCategoryName(F.col("category")))

dfFiltered = dfCateName.filter(dfCateName['category'] > 5)

df = dfFiltered.groupBy('category_name').count()

end_time = time.time()
execution_time = end_time - start_time
print("Temps d'exécution scala_udf avec persiste après la transformation de colonnes :", execution_time, "secondes")
