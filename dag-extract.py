from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parámetros generales
bucket_name = "datalake"
source = "mysql"
tables = [
    "big_chance_team",
    "clean_sheet_team",
    "effective_clearance_team",
    "expected_goals_team",
    "ontarget_scoring_att_team",
    "penalty_won_team",
    "possession_won_att",
    "team_goals_per_match",
    "touches_in_opp_box_team",
    "player_expected_assists",
    "expected_goals_conceded_team",
    "goals_conceded_team_match",
    "interception_team",
    "penalty_conceded_team",
    "saves_team",
    "won_tackle_team",
    "fk_foul_lost_team",
    "interception_team",
    "total_yel_card_team",
]

# Función para obtener datos de MySQL usando MySqlHook y guardar cada tabla en un archivo CSV
def fetch_and_upload_data(**context):
    table_status = {}

    try:
        # Crear instancia del hook MySQL
        logger.info("Conectando a MySQL usando MySqlHook...")
        mysql_hook = MySqlHook(mysql_conn_id="mysql_database_bets")

        # Crear instancia del hook S3
        s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")

        # Obtener la fecha actual para usarla en la estructura de la carpeta en S3
        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        for table_name in tables:
            try:
                # Obtener datos de cada tabla y guardarlos como CSV
                query = f"SELECT * FROM {table_name}"
                df = mysql_hook.get_pandas_df(query)
                logger.info(f"Se obtuvieron {len(df)} filas de la tabla {table_name}.")

                if not df.empty:
                    # Crear nombre de archivo y guardar como CSV
                    timestamp = current_date.strftime("%Y%m%d")
                    file_name = f"/tmp/{table_name}_{timestamp}.csv"
                    df.to_csv(file_name, index=False)
                    logger.info(
                        f"Datos de la tabla {table_name} guardados en {file_name}."
                    )

                    # Definir la ruta en S3 utilizando la estructura `raw/YYYY/MM/DD`
                    s3_key = f"raw/{source}/{table_name}/year={year}/month={month}/day={day}/{table_name}_{timestamp}.csv"

                    # Subir el archivo CSV a S3
                    logger.info(
                        f"Subiendo archivo '{file_name}' al bucket S3 '{bucket_name}' con key '{s3_key}'..."
                    )
                    s3_hook.load_file(
                        filename=file_name,
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True,
                    )
                    logger.info(f"Archivo '{file_name}' subido exitosamente a S3.")

                    # Indicar que la tabla se procesó correctamente
                    table_status[table_name] = "SUCCESS"
                else:
                    logger.warning(f"No se obtuvieron datos de la tabla {table_name}.")
                    table_status[table_name] = "ERROR"

            except Exception as e:
                logger.error(f"Error al procesar la tabla '{table_name}': {e}")
                table_status[table_name] = "ERROR"

            finally:
                # Eliminar el archivo CSV local después de subirlo a S3
                if os.path.exists(file_name):
                    os.remove(file_name)

    except Exception as err:
        logger.error(
            f"Error al conectar con MySQL o durante el proceso de carga: {err}"
        )
        raise RuntimeError(f"Error durante el proceso de extracción o carga: {err}")

    # Pasar el estado de cada tabla a través de XCom para la siguiente tarea
    context["ti"].xcom_push(key="table_status", value=table_status)


# Función para crear un archivo de estado (_SUCCESS o _ERROR) por cada tabla
def create_status_files(**context):
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")
    current_date = datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    # Obtener el estado de cada tabla del XCom
    table_status = context["ti"].xcom_pull(
        key="table_status", task_ids="fetch_and_upload_data"
    )

    # Definir la ruta base para los archivos de estado
    status_file_key_prefix = f"raw/{source}/status/year={year}/month={month}/day={day}/"

    # Crear un archivo de estado para cada tabla
    for table_name, status in table_status.items():
        status_file_key = f"{status_file_key_prefix}{table_name}_{status}"
        logger.info(f"Creando archivo de estado '{status_file_key}'.")

        # Crear archivo de estado temporal
        status_file_path = f"/tmp/{os.path.basename(status_file_key)}"
        with open(status_file_path, "w") as status_file:
            status_file.write(f"Tabla: {table_name}\nEstado: {status}\n")

        # Subir el archivo de estado a S3
        try:
            s3_hook.load_file(
                filename=status_file_path,
                key=status_file_key,
                bucket_name=bucket_name,
                replace=True,
            )
            logger.info(
                f"Archivo de estado '{status_file_key}' subido exitosamente a S3."
            )
        except Exception as e:
            logger.error(
                f"Error al subir el archivo de estado '{status_file_path}' a S3: {e}"
            )
            raise RuntimeError(
                f"Error al subir el archivo de estado '{status_file_path}' a S3: {e}"
            )
        finally:
            # Eliminar el archivo temporal local
            if os.path.exists(status_file_path):
                os.remove(status_file_path)


# Definir DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 18),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "a_Extract_football_analytics_extract_to_raw",
    default_args=default_args,
    description="DAG para obtener datos de varias tablas MySQL y subirlos a S3 con estructura de raw y crear archivo de estado por tabla",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["etl", "data-pipeline", "football-analytics"],
) as dag:

    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_data",
        python_callable=fetch_and_upload_data,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    create_status_files_task = PythonOperator(
        task_id="create_status_files",
        python_callable=create_status_files,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Tarea para disparar el DAG de transformación
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="b_Transform_football_analytics_transform_to_refined",  # Nombre del DAG que quieres disparar
        wait_for_completion=False,  # Esperar a que el DAG finalice (opcional)
    )

    # Definir la secuencia de tareas
    fetch_and_upload_task >> create_status_files_task >> trigger_transform_dag
