'''
=================================================
Milestone 3

Nama  : Rolando Krisnanto
Batch : CODA-RMT-021

Program ini dibuat untuk melakukan proses TRANSFORM terhadap dataset yang digunakan menggunakan PySpark. Data didapatkan dari hasil extract.py, dan dilakukan cleaning sesuai kesimpulan eksplorasi data di notebook, lalu mulai buat fact dan dimension table.
=================================================
'''

# Import SparkSession dan function PySpark yang akan dipakai
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Hasil extract dari extract.py disimpan di folder extract_result
extract_result = "/opt/airflow/data/extract_result"

# Path lokasi penyimpanan hasil transform
transform_result = "/opt/airflow/data/transform_result"


def transform(data):

    # Hapus baris yang datanya duplikat
    data = data.dropDuplicates()

    # Ubah datatype kolom "Date" menjadi date
    data = data.withColumn("Date", F.to_date(F.col("Date"), "yyyy-MM-dd"))

    # Buat kolom Transaction_ID sebagai identifier unik tiap baris
    ## Gunakan monotonically_increasing_id() untuk membuat ID unik
    ## Selanjutnya gunakan Window function untuk mengelompokan ID unik sebelumnya menjadi Transaction_ID
    ## Lalu gunakan row_number() untuk membuat urutan sesuai row dan dijadikan Transaction_ID
    trs_id = Window.orderBy(F.monotonically_increasing_id())
    data = data.withColumn("Transaction_ID", F.row_number().over(trs_id))

    # Buat Dimension Table
    ## Product Dimension, buat kolom Product_ID sebagai identifier unik tiap baris
    dim_product = data.select("Product_Category", "Sub_Category", "Product").distinct()
    dim_product = dim_product.withColumn("Product_ID", F.row_number().over(Window.orderBy("Product")))

    ## Customer Dimension, buat kolom Customer_ID sebagai identifier unik tiap baris
    dim_customer = data.select("Customer_Age", "Age_Group", "Customer_Gender").distinct()
    dim_customer = dim_customer.withColumn("Customer_ID", F.row_number().over(Window.orderBy("Customer_Age")))

    ## Location Dimension, buat kolom Location_ID sebagai identifier unik tiap baris
    dim_location = data.select("Country", "State").distinct()
    dim_location = dim_location.withColumn("Location_ID", F.row_number().over(Window.orderBy("Country", "State")))

    ## Time Dimension, buat kolom Time_ID sebagai identifier unik tiap baris
    dim_time = data.select("Date", "Day", "Month", "Year").distinct()
    dim_time = dim_time.withColumn("Time_ID", F.row_number().over(Window.orderBy("Date")))

    # Bangun Fact Table
    ## Fact Sales, buat 4 join dari masing-masing dimension ke fact_sales, untuk menaruh ID setiap dimension
    ## Untuk jaga-jaga, gunakan tipe left join agar tidak ada data yang hilang
    fact_sales = data.join(dim_product, on=["Product_Category", "Sub_Category", "Product"], how="left")
    fact_sales = fact_sales.join(dim_customer, on=["Customer_Age", "Age_Group", "Customer_Gender"], how="left")
    fact_sales = fact_sales.join(dim_location, on=["Country", "State"], how="left")
    fact_sales = fact_sales.join(dim_time, on=["Date", "Day", "Month", "Year"], how="left")

    # Buang semua kolom yang sudah tidak digunakan di fact table, sisakan ID masing-masing dimension dan measurementnya
    ## yang tersisa hanya kolom Transaction_ID, Product_ID, Customer_ID, Location_ID, Time_ID, Order_Quantity, Unit_Cost, Unit_Price, Profit, Cost, Revenue
    fact_sales = fact_sales.drop(
        "Product_Category", "Sub_Category", "Product",
        "Customer_Age", "Age_Group", "Customer_Gender",
        "Country", "State",
        "Date", "Day", "Month", "Year"
    )

    # Buat return kelima tabel dalam sebuah dictionary
    return {
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_location": dim_location,
        "dim_time": dim_time,
        "fact_sales": fact_sales
    }


# Buat SparkSession
spark = SparkSession.builder.appName("TransformData").getOrCreate()

# Baca hasil extract
data = spark.read.csv(extract_result, header=True, inferSchema=True)

# Bersihkan data dan bangun star schema
tables = transform(data)

# Simpan kelima tabel ke folder transform_result dan dibuatkan subfolder untuk tiap tabelnya
tables["dim_product"].write.mode("overwrite").option("header", True).csv(f"{transform_result}/dim_product")
tables["dim_customer"].write.mode("overwrite").option("header", True).csv(f"{transform_result}/dim_customer")
tables["dim_location"].write.mode("overwrite").option("header", True).csv(f"{transform_result}/dim_location")
tables["dim_time"].write.mode("overwrite").option("header", True).csv(f"{transform_result}/dim_time")
tables["fact_sales"].write.mode("overwrite").option("header", True).csv(f"{transform_result}/fact_sales")


'''
Refferences:
- pyspark.sql.functions.monotonically_increasing_id: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.monotonically_increasing_id.html
- pyspark.sql.Window: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html
- pyspark.sql.functions.row_number: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html

'''