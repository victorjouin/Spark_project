import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
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

# Load data
df = spark.read.csv('./src/resources/exo4/sell.csv', header=True)
start_time = time.time()
# Convert 'category' column to IntegerType
df = df.withColumn('category', df['category'].cast(IntegerType()))

# Use UDF to add new column
df = df.withColumn('category_name', category_name_udf(df['category']))

df.show()
end_time = time.time()
print(" no udf Execution time: {} seconds".format(end_time - start_time))