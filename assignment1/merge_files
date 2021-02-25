from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.appName("DatSparkByExample.com").getOrCreate()
    

nc = spark.read.csv(r"F:/project/nonConfidential.csv")

c = spark.read.parquet(r"F:/project/confidential.snappy.parquet")
merged = c.unionAll(nc)

def mergedData():
    return merged

def virginiaData():
    virginia_data = merged.filter(merged.State=="VA")
    return virginia_data

