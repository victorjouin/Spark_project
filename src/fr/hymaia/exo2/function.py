import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def filter_majeur(clients_df):
    if clients_df.filter(f.col("age").cast("int").isNull()).count() > 0:
        raise TypeError("The 'age' column must contain only numeric data")
    result = clients_df.filter(f.col("age") >= 18)
    return result

def join_villes(clients_df, villes_df):
    clients_df.join(villes_df, "zip", "left")
    return clients_df.join(villes_df, "zip", "left")

def add_departement(df):
    df = df.withColumn("departement", f.when(f.col("zip").startswith("20"), f.when(f.col("zip") <= 20190, "2A").otherwise("2B")).otherwise(f.substring(f.col("zip"), 0, 2)))
    return df

def pop_depart(df):
    try:
        df = df.groupBy("departement").agg(f.count("*").alias("nb_people")).orderBy(["nb_people", "departement"])
        if df.filter(f.col("nb_people").cast("int").isNull()).count() > 0:
            raise TypeError("'nb_people' is not numeric type")
        return df
    except AnalysisException:
        raise TypeError("'departement' is not string type")

def write_csv(df, chemin):
    df.write.csv(chemin, mode="overwrite", header=True)
    




