#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', required=True, help='Path to CSV file')
parser.add_argument('--separator', default=',', help='CSV separator')
parser.add_argument('--limit', type=int, default=10, help='Number of rows to show')

args = parser.parse_args()

spark = (
    SparkSession.builder
        .appName("ReadCSV")
        .getOrCreate()
)

try:
    df = (
        spark.read 
            .option("header", "true") 
            .option("inferSchema", "true") 
            .option("delimiter", args.separator) 
            .csv(args.input_path)
    )
    
    print(f"File: {args.input_path}")
    print(f"Total rows: {df.count()}")
    print(f"Schema:")
    df.printSchema()
    
    print(f"\nFirst {args.limit} rows:")
    df.show(args.limit, truncate=False)
    
except Exception as e:
    print(f"Error reading CSV: {e}")
    raise
finally:
    spark.stop()