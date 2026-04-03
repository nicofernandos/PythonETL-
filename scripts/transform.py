from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

def run_transform():
    spark = SparkSession.builder \
        .appName("KaggleToPostgress") \
        .config("spark.jars", "/media/farhan/DATA PRIBADI/Project/pipeline/jars/postgresql-42.7.10.jar") \
        .getOrCreate()
    
    print("[*] Memulai proses transform dengan PySpark")
    
    raw_path = "/media/farhan/DATA PRIBADI/Project/pipeline/data/raw/*.csv"
    df = spark.read.csv(raw_path, header=True, inferSchema=True)

    data_mart = df.withColumnRenamed("suicides/100k pop","suicides_100k") \
                  .withColumnRenamed("country-year","country_year") \
                  .withColumnRenamed(" HDI for year","hdi_year")
    
    data_mart.show(10)
    
    print("[*] Proses transform selesai, data siap untuk dimuat ke PostgreSQL")
    
    jdbc_url = "jdbc:postgresql://localhost:5432/kaggle"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver" : "org.postgresql.Driver"
    }
    
    data_mart.write.jdbc(url=jdbc_url, table="dm_suicide_summary", mode="overwrite", properties=db_properties)
    print("[OK] Data Mart berhasil dibuat di PostgreSQL!")

if __name__ == "__main__":
    run_transform()