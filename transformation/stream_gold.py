from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# 1. Initialize Spark 4.x with Kafka 4.x and Postgres Jar
spark = SparkSession.builder \
    .appName("KafkaToPostgresStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2") \
    .config("spark.jars", "/home/ramano/Documents/projects/tech-job/drivers/postgresql-42.7.2.jar") \
    .getOrCreate()

# 2. Define Schema (Must match your Kafka Producer JSON)
schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("skills", ArrayType(StringType()), True)
])

# 3. Read Stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tech_jobs_stream") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Write to Postgres
def write_to_postgres(batch_df, batch_id):
    # This prints to your terminal so you can see it working live
    print(f"Processing Batch ID: {batch_id}")
    batch_df.show() 
    
    # Save to your actual Database
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tech_jobs") \
        .option("dbtable", "gold_job_skills") \
        .option("user", "de_user") \
        .option("password", "data123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 6. Start the Query
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

# This line keeps the script running forever (waiting for Kafka)
query.awaitTermination()