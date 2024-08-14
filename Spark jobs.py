'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("End-to-End Data Pipeline") \
    .getOrCreate()

# 1. Data Ingestion
# Read data from a CSV file
df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)
df.show()

# 2. Data Processing
# Drop rows with missing values
df_cleaned = df.dropna()

# Transform data: Add a new column based on conditions
df_transformed = df_cleaned.withColumn("new_column", when(col("existing_column") > 100, "High").otherwise("Low"))
df_transformed.show()

# 3. Data Aggregation
# Group by a column and calculate the average of another column
df_aggregated = df_transformed.groupBy("group_column").agg({"value_column": "avg"})
df_aggregated.show()

# 4. Data Loading
# Write the processed data to a Parquet file
df_aggregated.write.parquet("path/to/output/data.parquet")

# 5. Data Analysis
# Register the dataframe as a temporary SQL table
df_aggregated.createOrReplaceTempView("aggregated_data")

# Run an SQL query
result = spark.sql("SELECT * FROM aggregated_data WHERE avg(value_column) > 50")
result.show()

# 6. Data Quality Testing
# Check for null values in the dataframe
null_counts = df_aggregated.select([count(when(col(c).isNull(), c)).alias(c) for c in df_aggregated.columns])
null_counts.show()

# 7. Performance Optimization
# Cache the dataframe to optimize performance
df_aggregated.cache()

# Perform some actions to utilize the cache
df_aggregated.count()

# Stop the Spark session
spark.stop()

'''
