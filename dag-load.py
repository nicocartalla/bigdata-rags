from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import sqlalchemy
from sqlalchemy import create_engine
from tempfile import NamedTemporaryFile
from datetime import datetime
import pandas as pd
import logging
import os

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de S3
s3_config = {
    "bucket_name": "datalake",
}

# Función para verificar si el archivo _SUCCESS existe en S3
def verify_success_file():
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")

    # Calcular fecha actual para construir la ruta del archivo _SUCCESS
    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    success_key = (
        f"stage/mysql/status/year={year}/month={month}/day={day}/transform_SUCCESS"
    )

    if not s3_hook.check_for_key(key=success_key, bucket_name=s3_config["bucket_name"]):
        logger.error(f"El archivo de estado '{success_key}' no existe en S3.")
        raise RuntimeError(f"El archivo de estado '{success_key}' no existe en S3.")
    logger.info(f"El archivo de estado '{success_key}' existe en S3.")


# Función para descargar un archivo CSV desde S3
def download_csv_from_s3(s3_file: str) -> str:
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")
    s3_client = s3_hook.get_conn()

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    s3_key = f"stage/mysql/{s3_file}/year={year}/month={month}/day={day}/{s3_file}_{year}{month}{day}.csv"
    local_path = f"/tmp/{s3_file}.csv"

    try:
        logger.info(f"Descargando archivo '{s3_key}' desde S3 a '{local_path}'...")
        bucket = s3_config["bucket_name"]
        s3_client.download_file(bucket, s3_key, local_path)
        logger.info(f"Archivo '{s3_file}' descargado exitosamente en '{local_path}'.")
        return local_path
    except Exception as e:
        logger.error(f"Error al descargar el archivo '{s3_key}' desde S3: {e}")
        return None


# Función para crear una conexión SQLAlchemy a partir de MySqlHook
def create_sqlalchemy_engine():
    mysql_hook = MySqlHook(mysql_conn_id="mysql_database_bets")
    connection_details = mysql_hook.get_connection("mysql_database_bets")

    # Construir la cadena de conexión SQLAlchemy
    connection_string = f"mysql+pymysql://{connection_details.login}:{connection_details.password}@{connection_details.host}:{connection_details.port}/{connection_details.schema}"
    logger.info(f"Conexión SQLAlchemy creada exitosamente.")
    return create_engine(connection_string)


# Función para guardar un archivo CSV desde S3 en una tabla de MySQL
def save_csv_from_s3_to_mysql(s3_file: str, table_name: str):
    # Descargar archivo CSV desde S3
    temp_file = download_csv_from_s3(s3_file)
    if temp_file is None:
        logger.error(f"No se pudo descargar el archivo {s3_file}.")
        return

    # Leer CSV con pandas
    try:
        df = pd.read_csv(temp_file)
        logger.info(f"Datos cargados en DataFrame desde {temp_file}.")
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {e}")
        return

    # Guardar los datos en MySQL
    engine = create_sqlalchemy_engine()
    try:
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        logger.info(
            f"Datos guardados exitosamente en la tabla '{table_name}' de MySQL."
        )
    except Exception as e:
        logger.error(f"Error al guardar datos en MySQL: {e}")
    finally:
        engine.dispose()

    # Eliminar el archivo temporal
    os.remove(temp_file)
    logger.info(f"Archivo temporal '{temp_file}' eliminado.")


# Definir el DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "c_Load_football_analytics_load_to_exploration",
    default_args=default_args,
    description="DAG para cargar datos desde S3 (stage) a MySQL",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "data-pipeline", "football-analytics"],
)

# Tarea para verificar que el archivo _SUCCESS exista
task_verify_success_file = PythonOperator(
    task_id="verify_success_file",
    python_callable=verify_success_file,
    dag=dag,
)

# Tarea para guardar los datos de ataque en MySQL
task_save_attack_data = PythonOperator(
    task_id="save_attack_data_to_mysql",
    python_callable=save_csv_from_s3_to_mysql,
    op_kwargs={"s3_file": "attack_data", "table_name": "attack"},
    dag=dag,
)

# Tarea para guardar los datos de defensa en MySQL
task_save_defense_data = PythonOperator(
    task_id="save_defense_data_to_mysql",
    python_callable=save_csv_from_s3_to_mysql,
    op_kwargs={"s3_file": "defense_data", "table_name": "defense"},
    dag=dag,
)

# Tarea para guardar los datos de disciplina en MySQL
task_save_discipline_data = PythonOperator(
    task_id="save_discipline_data_to_mysql",
    python_callable=save_csv_from_s3_to_mysql,
    op_kwargs={"s3_file": "discipline_data", "table_name": "discipline"},
    dag=dag,
)

# Definir dependencias
task_verify_success_file >> [
    task_save_attack_data,
    task_save_defense_data,
    task_save_discipline_data,
]
