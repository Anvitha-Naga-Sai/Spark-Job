"""
Different Ways to Optimize Spark jobs. Have written sample code snippets in order to optimize them.
1. Minimize Data Shuffling

Use Narrow Transformations
Narrow transformations like `map` and `filter` do not require data shuffling across the network.

# Narrow transformation example
df_filtered = df.filter(df['age'] > 30)
```

Optimize Join Operations
Use broadcast joins for small datasets to avoid shuffling large datasets.
from pyspark.sql.functions import broadcast

# Assuming df1 is large and df2 is small
df1 = spark.read.csv("path/to/large_file.csv", header=True, inferSchema=True)
df2 = spark.read.csv("path/to/small_file.csv", header=True, inferSchema=True)

# Broadcast join
joined_df = df1.join(broadcast(df2), df1["id"] == df2["id"])
```

2. Use DataFrame/Dataset API
DataFrames and Datasets come with built-in optimizations.
# Using DataFrame API
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df_filtered = df.filter(df['age'] > 30).select('name', 'age')
```

3. Cache and Persist Data
Cache or persist intermediate results to avoid recomputation.
# Cache DataFrame
df_cached = df.cache()
df_cached.count()  # Trigger the cache

# Persist DataFrame with specific storage level
from pyspark import StorageLevel
df_persisted = df.persist(StorageLevel.MEMORY_AND_DISK)
df_persisted.count()  # Trigger the persist
```

 4. Use Partitioning
Repartition or coalesce DataFrames to optimize the number of partitions.
# Repartition DataFrame
df_repartitioned = df.repartition(10)
# Coalesce DataFrame (reduce number of partitions)
df_coalesced = df.coalesce(5)
```

5. Avoid UDFs When Possible
Use built-in functions instead of User-Defined Functions (UDFs) for better performance.
from pyspark.sql.functions import col
# Using built-in functions
df_filtered = df.filter(col('age') > 30)
```

 6. Optimize File Formats
Use efficient file formats like Parquet or ORC for better performance.
# Write DataFrame to Parquet
df.write.parquet("path/to/output.parquet")

# Read DataFrame from Parquet
df_parquet = spark.read.parquet("path/to/output.parquet")
```

 7. Tune Spark Configurations
Adjust Spark configurations for better performance.
# Set Spark configurations
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.executor.memory", "4g")
```

8. Use Columnar Storage
Use columnar storage formats like Parquet for better compression and performance.
# Write DataFrame to Parquet
df.write.parquet("path/to/output.parquet")
```

 9. Enable Predicate Pushdown
Ensure predicate pushdown is enabled to filter data at the source.
# Read DataFrame with predicate pushdown
df = spark.read.parquet("path/to/file.parquet").filter(col('age') > 30)
```
 10. Monitor and Tune Jobs
Use Spark UI and logs to monitor and tune your jobs.
# Enable Spark UI
spark = SparkSession.builder \
    .appName("OptimizedSparkJob") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "path/to/logs") \
    .getOrCreate()
```
"""