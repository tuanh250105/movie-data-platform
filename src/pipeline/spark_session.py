"""
Spark Session Builder Module.
Initializes PySpark Session with Apache Iceberg and S3/MinIO configurations,
with automatic fallback to local PySpark SQL engine.
"""

import os
import sys

def get_spark_session(app_name: str = "Movies_Lakehouse_Pipeline"):
    """
    Creates and returns a PySpark Session configured for Apache Iceberg and Parquet processing.
    """
    try:
        from pyspark.sql import SparkSession
        
        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "data/silver/warehouse")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.driver.memory", "4g")
        )
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("PySpark Lakehouse Session initialized successfully.")
        return spark
        
    except ImportError:
        print("PySpark is not installed in the current Python environment.")
        print("Falling back to Pandas/DuckDB Data Engine for Silver Layer Transformation.")
        return None

if __name__ == "__main__":
    spark = get_spark_session()
    if spark:
        print("Spark Version:", spark.version)
