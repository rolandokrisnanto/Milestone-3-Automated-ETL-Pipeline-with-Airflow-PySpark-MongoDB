'''
=================================================
Milestone 3
 
Nama  : Rolando Krisnanto
Batch : CODA-RMT-021
 
Program ini dibuat untuk melakukan proses EXTRACT dari file dataset P2M3_rolando_krisnanto_data_raw.csv menggunakan PySpark.
=================================================
'''

# Jika belum install PySpark, silahkan install terlebih dahulu dengan perintah:
# pip install pyspark (jalankan di Terminal)

# Inisialisasi SparkSession
from pyspark.sql import SparkSession

# Path file dataset raw
raw_file_path = "/opt/airflow/data/P2M3_rolando_krisnanto_data_raw.csv"

# Path lokasi penyimpanan hasil extract
extract_result = "/opt/airflow/data/extract_result"
 
def load_data(file_path):
    # Buat SparkSession
    spark = SparkSession.builder.appName("ExtractData").getOrCreate()
 
    # Read CSV
    data = spark.read.csv(file_path, header=True, inferSchema=True)

    return data

data = load_data(raw_file_path)

# Simpan hasilnya ke file, supaya bisa dibaca oleh transform.py
data.write.mode("overwrite").option("header", True).csv(extract_result)

