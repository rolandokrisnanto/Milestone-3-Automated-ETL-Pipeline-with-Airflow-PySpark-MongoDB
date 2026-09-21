# Milestone 3 — Automated ETL Pipeline: Europe Bike Sales (PySpark + Airflow + MongoDB)

Data pipeline automation yang meng-extract, memvalidasi, mentransformasi, dan memuat data transaksi penjualan sepeda ke dalam data warehouse berskema star schema di MongoDB, dijadwalkan otomatis dengan Apache Airflow.

**Nama:** Rolando Krisnanto — **Batch:** CODA-RMT-021

## Deskripsi

Sebagai simulasi peran Data Engineer, project ini membangun sistem automasi ETL yang mengombinasikan PySpark (pemrosesan data terdistribusi), Great Expectations (validasi kualitas data), Apache Airflow (orkestrasi & penjadwalan), dan MongoDB (data warehouse NoSQL).

## Dataset

- **Dataset:** transaksi penjualan sepeda (Europe Bike Sales), ±113.000 baris
- **Kolom:** Date, Customer_Age, Age_Group, Customer_Gender, Country, State, Product_Category, Sub_Category, Product, Order_Quantity, Unit_Cost, Unit_Price, Profit, Cost, Revenue, dll.

## Data Validation — Great Expectations

7 Expectations dikumpulkan dalam satu Expectation Suite (`europe-bike-sales-suite`) dan seluruhnya `success: true`:

| # | Expectation | Tujuan |
|---|---|---|
| 1 | `ExpectColumnValuesToBeUnique` | `Transaction_ID` tidak boleh duplikat |
| 2 | `ExpectColumnValuesToBeBetween` | `Customer_Age` harus di antara 0–90 |
| 3 | `ExpectColumnValuesToBeInSet` | `Customer_Gender` hanya boleh "M" atau "F" |
| 4 | `ExpectColumnValuesToBeInTypeList` | Kolom `Date` bertipe `datetime64[ns]` |
| 5 | `ExpectColumnPairValuesAToBeGreaterThanB` | `Unit_Price` harus lebih besar dari `Unit_Cost` |
| 6 | `ExpectColumnValueLengthsToBeBetween` | Panjang nama `Product` antara 5–75 karakter |
| 7 | `ExpectTableRowCountToBeBetween` | Jumlah baris tabel antara 100.000–120.000 |

## Arsitektur Pipeline

```
Extract (PySpark)  →  Transform (PySpark, star schema)  →  Load (PyMongo → MongoDB Atlas)
                     orkestrasi & penjadwalan: Apache Airflow (Docker)
```

### 1. Extract — `scripts/extract.py`
Membaca file CSV mentah dengan PySpark (`spark.read.csv`) dan menyimpan hasilnya sebagai file perantara untuk tahap transform.

### 2. Transform — `scripts/transform.py`
- Menghapus baris duplikat dan mengonversi kolom `Date` ke tipe date
- Membuat `Transaction_ID` unik dengan `monotonically_increasing_id()` + window function
- Membangun **star schema**:
  - `dim_product` — Product_Category, Sub_Category, Product
  - `dim_customer` — Customer_Age, Age_Group, Customer_Gender
  - `dim_location` — Country, State
  - `dim_time` — Date, Day, Month, Year
  - `fact_sales` — hasil join keempat dimensi, menyisakan ID tiap dimensi + measure (Order_Quantity, Unit_Cost, Unit_Price, Profit, Cost, Revenue)

### 3. Load — `scripts/load.py`
Memuat kelima tabel hasil transform ke MongoDB Atlas (database `milestone3_dw`) menggunakan PyMongo, masing-masing tabel menjadi satu collection.

## Orkestrasi — Airflow DAG

DAG `P2M3_rolando_krisnanto_DAG` menjalankan 3 task berurutan `extract → transform → load`, dijadwalkan dengan cron `10,20,30 9 * * 6` (tiap Sabtu jam 09:10, 09:20, dan 09:30 WIB), mulai 1 November 2024, dijalankan di atas Airflow yang di-containerize dengan Docker (image custom berbasis `apache/airflow:2.3.4` + Java 11 untuk PySpark).

## Tech Stack

Python · PySpark · Apache Airflow · MongoDB · PyMongo · Great Expectations · Docker

## Struktur Project

```
├── P2M3_rolando_krisnanto.ipynb                       # EDA & validasi data dengan Great Expectations
├── dags/
│   └── P2M3_rolando_krisnanto_DAG.py                  # DAG Airflow (extract → transform → load)
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── data/
│   └── P2M3_rolando_krisnanto_data_raw.csv            # Dataset Raw
├── Dockerfile                                         # Image Airflow + PySpark (Java 11)
├── airflow.yaml                                       # Docker Compose untuk Airflow + Postgres
├── requirements.txt
├── P2M3_rolando_krisnanto_screenshot_mongo.jpg        # Bukti data tersimpan di MongoDB Atlas
├── P2M3_rolando_krisnanto_DAG_graph.jpg               # Bukti DAG berjalan di Airflow
└── README.md
```

## Cara Menjalankan

1. Siapkan file `.env` (konfigurasi untuk environmentnya) / gunakan dan rubah `env_example.txt` sebagai file `.env` nantinya
2. Jalankan Airflow + Spark via Docker Compose:
   ```
   docker compose -f airflow.yaml up -d --build
   ```
3. Buka Airflow UI, aktifkan/trigger DAG `P2M3_rolando_krisnanto_DAG`
4. Cek hasilnya di MongoDB Atlas pada database `milestone3_dw`
