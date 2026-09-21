'''
=================================================
Milestone 3

Nama  : Rolando Krisnanto
Batch : CODA-RMT-021

Program ini dibuat untuk melakukan proses LOAD dataset yang sudah di transform ke dalam MongoDB.
=================================================
'''

# Import SparkSession untuk membaca hasil transform dan MongoClient dari pymongo untuk koneksi ke MongoDB
from pyspark.sql import SparkSession
from pymongo import MongoClient

# Path hasil dari transform yang akan digunakan untuk load ke MongoDB
transform_result = "/opt/airflow/data/transform_result"

# =================================================
# CARA KONEK KE MONGODB:
# 1. Buat cluster gratis di MongoDB Atlas: https://www.mongodb.com/cloud/atlas/register
# 2. Di Atlas, buka Database Access > Add New Database User, buat username & password sendiri
# 3. Di Network Access, allow IP (0.0.0.0/0 untuk testing, atau IP spesifik untuk lebih aman)
# 4. Buka Database > Connect > Drivers, copy connection string yang formatnya seperti di bawah
# 5. Ganti <username>, <password>, dan <cluster-host> di bawah ini dengan milikmu sendiri
# =================================================
client = MongoClient("mongodb+srv://<username>:<password>@<cluster-host>/?appName=<app-name>")

# Pilih database yang dituju, jika belum ada akan generate otomatis oleh MongoDB
db_client = client['milestone3_dw']

# Buat SparkSession untuk baca file csv hasil transform
spark = SparkSession.builder.appName("LoadData").getOrCreate()

# Load tiap tabel satu-satu ke MongoDB
## dim_product
spark_df = spark.read.csv(f"{transform_result}/dim_product", header=True, inferSchema=True)
document_list = [row.asDict() for row in spark_df.collect()]
db_client["dim_product"].insert_many(document_list)

## dim_customer
spark_df = spark.read.csv(f"{transform_result}/dim_customer", header=True, inferSchema=True)
document_list = [row.asDict() for row in spark_df.collect()]
db_client["dim_customer"].insert_many(document_list)

## dim_location
spark_df = spark.read.csv(f"{transform_result}/dim_location", header=True, inferSchema=True)
document_list = [row.asDict() for row in spark_df.collect()]
db_client["dim_location"].insert_many(document_list)

## dim_time
spark_df = spark.read.csv(f"{transform_result}/dim_time", header=True, inferSchema=True)
document_list = [row.asDict() for row in spark_df.collect()]
db_client["dim_time"].insert_many(document_list)

## fact_sales
spark_df = spark.read.csv(f"{transform_result}/fact_sales", header=True, inferSchema=True)
document_list = [row.asDict() for row in spark_df.collect()]
db_client["fact_sales"].insert_many(document_list)
