from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.types import IntegerType
import time

# Create SparkSession with the JAR file in the configuration
spark = SparkSession.builder \
    .appName("exo4") \
    .master("local[*]") \
    .config('spark.jars', './src/resources/exo4/udf.jar') \
    .getOrCreate()

# Define Python function to wrap the Scala UDF
def category_name(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

# Load data
df = spark.read.csv('./src/resources/exo4/sell.csv', header=True)

start_time = time.time()
# Convert 'category' column to IntegerType
df = df.withColumn('category', df['category'].cast(IntegerType()))

# Use UDF to add new column
df = df.withColumn('category_name', category_name(df['category']))

df.show()
end_time = time.time()
print(" scala Execution time: {} seconds".format(end_time - start_time))