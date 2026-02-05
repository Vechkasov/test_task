import argparse
import sys
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--table', required=True, help='Table name to read')
parser.add_argument('--limit', type=int, default=10, help='Limit rows to show')
args = parser.parse_args()

spark = (
    SparkSession.builder
        .appName(f"Read_{args.table}")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.0.jar")
        .getOrCreate()
)

try:
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    
    df = (
        spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", args.table)
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .load()
    )
    
    print(f"Successfully read table: {args.table}")
    print(f"Total rows: {df.count()}")
    print("\nSchema:")
    df.printSchema()
    
    print(f"\nFirst {args.limit} rows:")
    df.show(args.limit, truncate=False)
    
    columns = df.columns
    row_count = df.count()
    
    print(f"\nScript completed successfully!")
    print(f"   Table: {args.table}")
    print(f"   Columns: {len(columns)}")
    print(f"   Rows: {row_count}")
    
    sys.exit(0)
    
except Exception as e:
    print(f"Error reading table {args.table}: {e}")
    sys.exit(1)
finally:
    spark.stop()
    print("Spark session stopped")