import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

# Créer la session Spark
spark = SparkSession.builder.appName("exo4_scala")\
        .master("local[*]")\
        .config('spark.jars', 'src/resources/exo4/udf.jar')\
        .getOrCreate()

start_time = time.time()

# Définir la fonction addCategoryName pour appeler l'UDF Scala
def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

# Charger les données à partir du fichier CSV
dfSell = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")

# Transformation de colonnes
dfTransformed = dfSell.withColumn("category", dfSell['category'].cast("int"))
dfTransformed = dfTransformed.withColumn("price", dfTransformed['price'].cast("double"))

# Persiste après la transformation de colonnes
dfTransformed.persist()

# Ajouter une colonne category_name en utilisant l'UDF Scala
dfCateName = dfTransformed.withColumn("category_name", addCategoryName(F.col("category")))

# Filtrer les données pour les catégories supérieures à 5
dfFiltered = dfCateName.filter(dfCateName['category'] > 5)

# Effectuer le groupement par category_name et compter
df = dfFiltered.groupBy('category_name').count()

# Mesurer le temps d'exécution
end_time = time.time()
execution_time = end_time - start_time
print("Temps d'exécution scala_udf avec persiste après la transformation de colonnes :", execution_time, "secondes")
