import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col



def process_gold_layer():
    print("Starting gold layer processing....")
    spark= SparkSession.builder\
    .appName("TechJobsMarket_GoldLayer")\
    .config("spark.sql.shuffle.partitions","4")\
    .getOrCreate()
    current_dir= os.path.dirname(__file__)
    project_root= os.path.dirname(current_dir)
    silver_path= os.path.join("storage", "silver").replace("\\","/")

    print(f"Reading data from {silver_path}")
    clean_df= spark.read.parquet(silver_path)
    skills_to_search=["Python","SQL", "Java", "Spark", "AWS"]
    gold_df= clean_df
    for skill in skills_to_search:
        regex_pattern= f"(?i)\\b{skill}\\b"
        column_name=f"requires_{skill.lower()}"
        gold_df=gold_df.withColumn(column_name,col("clean_description").rlike(regex_pattern))
    column_to_show=["job_title"]+[f"requires_{s.lower()}" for s in skills_to_search]
    print("\n----Gold Layer: Extraction skills")
    gold_df.select(*column_to_show).show(20,truncate=False)

if __name__=="__main__":
    process_gold_layer()
