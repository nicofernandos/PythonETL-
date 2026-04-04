import os
import sys 
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, avg

load_dotenv()

def run_transform():
    jar_path = os.getenv("JAR_PATH")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_url = os.getenv("DB_URL")
    db_driver = os.getenv("DB_DRIVER")

    spark = (SparkSession.builder
        .appName("KaggleToPostgres")
        .config("spark.jars", jar_path)
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print("[*] Memulai proses transformasi dengan kredensial dari .env")
     
    raw_path = "/media/farhan/DATA PRIBADI/Project/pipeline/data/raw/*.csv"
    df = spark.read.csv(raw_path, header=True, inferSchema=True)
    
    df_cleaned = (df
        .withColumnRenamed("suicides/100k pop", "suicides_100k")
        .withColumnRenamed("country-year", "country_year")
        .withColumnRenamed(" HDI for year", "hdi_year")
        .withColumnRenamed("gdp_per_capita ($)", "gdp_per_capita"))

    data_mart = (df_cleaned.groupBy("country", "year")
        .agg(
            sum("suicides_no").alias("total_suicides"),
            round(avg("gdp_per_capita"), 2).alias("avg_gdp_per_capita")
        )
        .orderBy("country", "year"))

    print("[*] Menjalankan Validasi Data Quality...")
    
    total_rows = data_mart.count()
    if total_rows == 0:
        print("[ERROR] Data Mart kosong! Proses Load dibatalkan.")
        return 
    null_data = data_mart.filter(col("country").isNull()).count()
    if null_data > 0:
        print(f"[FIX] Ditemukan {null_data} data NULL pada kolom country. Menghapus data tersebut...")
        data_mart = data_mart.dropna(subset=["country"])

    negative_check = data_mart.filter(col("total_suicides") < 0).count()
    if negative_check > 0:
        print(f"[ERROR] Ditemukan {negative_check} data dengan angka negatif! Load dibatalkan.")
        return

    print(f"[OK] Validasi berhasil. {total_rows} baris siap di-load.")

    print("[*] Preview Data Mart (10 baris):")
    data_mart.show(10, truncate=False)
    
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": db_driver
    }
    
    print("[*] Mengirim data ke database...")
    data_mart.write.jdbc(url=db_url, table="dm_suicide_summary", mode="overwrite", properties=db_properties)
    
    print("[OK] Data mart berhasil di-load menggunakan konfigurasi .env")

if __name__ == "__main__":
    run_transform()