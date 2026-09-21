'''
=================================================
Milestone 3
 
Nama  : Rolando Krisnanto
Batch : CODA-RMT-021
 
Program ini dibuat untuk melakukan automasi proses ETL menggunakan Airflow. 
DAG ini menjalankan 3 task berurutan: Extract -> Transform -> Load, memanggil script extract.py, transform.py, dan load.py yang ada di folder scripts/.
Penjadwalan dimulai 1 November 2024, dijalankan setiap hari Sabtu jam 09:10, 09:20, dan 09:30.
=================================================
'''
 
import datetime as dt
from datetime import timedelta
 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
 
# Konfigurasi default untuk DAG ini
default_args = {
    'owner': 'rolando',  # siapa yang punya pipeline ini
    'start_date': dt.datetime(2024, 11, 1),  # tanggal mulai pipeline ini dijalankan (2024-11-01)
    'retries': 1,  # berapa kali jika gagal akan dicoba lagi
    'retry_delay': dt.timedelta(minutes=5),  # berapa lama menunggu sebelum mencoba lagi
}
 
 
with DAG('P2M3_rolando_krisnanto_DAG',  # nama DAG
         default_args=default_args,
         schedule_interval='10,20,30 9 * * 6',  # tiap Sabtu, jam 09:10, 09:20, 09:30
         catchup=False,  # supaya tidak terpengaruh start_date, dimulai dari sekarang
         ) as dag:
 
    # Extract (run extract.py)
    python_extract = BashOperator(
        task_id='extract',
        bash_command='sudo -u airflow python /opt/airflow/scripts/extract.py'
    )
 
    # Transform (run transform.py)
    python_transform = BashOperator(
        task_id='transform',
        bash_command='sudo -u airflow python /opt/airflow/scripts/transform.py'
    )
 
    # Load (run load.py)
    python_load = BashOperator(
        task_id='load',
        bash_command='sudo -u airflow python /opt/airflow/scripts/load.py'
    )
 
# Urutan running (Extract -> Transform -> Load)
python_extract >> python_transform >> python_load
