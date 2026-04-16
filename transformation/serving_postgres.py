import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def build_gold_and_serve():
    current_dir= os.path.dirname(__file__)
    project_root=os.path.dirname(current_dir)
    silver_path= os.path.join(project_root,"storage","silver")
    jar_path= os.path.join(project_root,"postgresql-42.7.3.jar").replace("\\","/")
    spark= SparkSession.builder\
        .appName("TechJobMarket_GoldServing")\
        .config("spark.jars",jar_path)\
        .config("spark.sql.suffle.partitions","4")\
        .getOrCreate()
    clean_df= spark.read.parquet(silver_path)
    skills_to_search = ["Python", "SQL", "Java", "Spark", "AWS"]
    gold_df = clean_df
    
    for skill in skills_to_search:
        regex_pattern = f"(?i)\\b{skill}\\b" 
        column_name = f"requires_{skill.lower()}"
        gold_df = gold_df.withColumn(column_name, col("clean_description").rlike(regex_pattern))
        final_db_df= gold_df.drop("clean_description")
        jdbc_url = "jdbc:postgresql://localhost:5432/tech_jobs"
        connection_properties = {
            "user": "de_user",
            "password": "data123",
            "driver": "org.postgresql.Driver"
        }
        final_db_df.write.jdbc(
        url=jdbc_url, 
        table="gold_job_skills", 
        mode="overwrite", 
        properties=connection_properties
        )
        print("SUCCESS: Pipeline complete. Data is now served in PostgreSQL!")

if __name__=="__main__":
    build_gold_and_serve()
