import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from src.fr.hymaia.exo1.function import wordcount
def main():

    # local[*] means use all cores available
    master = "local[*]"
    app_name = "wordcount"

    spark = SparkSession.builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()

    df = spark.read.csv('src/resources/exo1/data.csv', header=True, inferSchema=True)
    wordcount_df = wordcount(df, 'text')
    wordcount_df.show()

    output_path = "data/exo1/output"
    print(f"Writing results to: {output_path}")

    wordcount_df.write.partitionBy("count").parquet(output_path, mode="overwrite")
    
    


if __name__ == '__main__':
    main()