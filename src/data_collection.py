#data_collection.py
from pyspark.sql import SparkSession

def load_data(file_path):
    """
    Loads the dataset from CSV using PySpark.
    """
    spark = SparkSession.builder \
        .appName("Ecommerce Sales Analysis") \
        .getOrCreate()

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print("✅ Data loaded successfully!")
    print(f"Total Records: {df.count()}")
    df.printSchema()
    return df, spark
