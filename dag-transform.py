from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
import os
import shutil
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum
from tempfile import NamedTemporaryFile

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
current_date = datetime.now()
year = current_date.strftime("%Y")
month = current_date.strftime("%m")
day = current_date.strftime("%d")


def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# Función para verificar que todos los archivos de estado sean `_SUCCESS`
def verify_all_success():
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")
    for table_name in tables:
        status_key = f"raw/{source}/status/year={year}/month={month}/day={day}/{table_name}_SUCCESS"
        if not s3_hook.check_for_key(key=status_key, bucket_name=bucket_name):
            logger.error(f"El estado de la tabla {table_name} no es '_SUCCESS'.")
            return False
    logger.info("Todas las tablas tienen estado '_SUCCESS'.")
    return True


# Función para descargar tablas desde S3
def download_tables():
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")
    s3_client = s3_hook.get_conn()  # Obtener el cliente de S3 de boto3

    for table_name in tables:
        s3_key = f"raw/{source}/{table_name}/year={year}/month={month}/day={day}/{table_name}_{year}{month}{day}.csv"
        local_path = f"/tmp/{table_name}.csv"

        try:
            # Descargar el archivo directamente al local_path usando boto3 client
            logger.info(f"Descargando archivo '{s3_key}' desde S3 a '{local_path}'...")
            bucket = bucket_name
            s3_client.download_file(bucket, s3_key, local_path)
            logger.info(
                f"Archivo '{table_name}' descargado exitosamente en '{local_path}'."
            )

        except Exception as e:
            logger.error(f"Error al descargar el archivo '{s3_key}' desde S3: {e}")
            raise RuntimeError(
                f"Error al descargar el archivo '{s3_key}' desde S3: {e}"
            )


def upload_to_stage(df, file_prefix):
    # Definir el directorio temporal donde se va a guardar el CSV
    temp_dir = f"/tmp/{file_prefix}_stage"
    os.makedirs(temp_dir, exist_ok=True)

    # Guardar el DataFrame como CSV en el directorio temporal
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")

    # Buscar el archivo CSV generado dentro del directorio
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
    part_file_path = os.path.join(temp_dir, part_file)

    # Definir la clave para la capa stage en S3
    current_date = datetime.now()
    stage_key = (
        f"stage/mysql/{file_prefix}/year={current_date.year}/"
        f"month={current_date.month:02}/day={current_date.day:02}/"
        f"{file_prefix}_{current_date.strftime('%Y%m%d')}.csv"
    )

    # Crear el cliente de S3 usando S3Hook
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")
    s3_client = s3_hook.get_conn()

    try:
        logger.info(
            f"Subiendo archivo transformado '{part_file_path}' a S3 en '{stage_key}'..."
        )
        # Subir el archivo usando el cliente de boto3
        s3_client.upload_file(part_file_path, bucket_name, stage_key)
        logger.info(f"Archivo '{stage_key}' subido exitosamente a S3.")
    except Exception as e:
        logger.error(f"Error al subir el archivo '{part_file_path}' a S3: {e}")
        raise RuntimeError(f"Error al subir el archivo '{part_file_path}' a S3: {e}")
    finally:
        # Eliminar el directorio temporal
        shutil.rmtree(temp_dir)


# Transformación del ataque
def transform_attack():
    spark = create_spark_session("Transform Attack Data")
    # Cargar las tablas descargadas desde la capa raw
    big_chance = spark.read.csv(
        f"/tmp/big_chance_team.csv", header=True, inferSchema=True
    )
    clean_sheet = spark.read.csv(
        f"/tmp/clean_sheet_team.csv", header=True, inferSchema=True
    )
    effective_clearance = spark.read.csv(
        f"/tmp/effective_clearance_team.csv", header=True, inferSchema=True
    )
    expected_goals = spark.read.csv(
        f"/tmp/expected_goals_team.csv", header=True, inferSchema=True
    )
    ontarget_scoring_att = spark.read.csv(
        f"/tmp/ontarget_scoring_att_team.csv", header=True, inferSchema=True
    )
    penalty_won = spark.read.csv(
        f"/tmp/penalty_won_team.csv", header=True, inferSchema=True
    )
    possession_won_att_3rd = spark.read.csv(
        f"/tmp/possession_won_att.csv", header=True, inferSchema=True
    )
    team_goals_per_match = spark.read.csv(
        f"/tmp/team_goals_per_match.csv", header=True, inferSchema=True
    )
    touches_in_opp_box = spark.read.csv(
        f"/tmp/touches_in_opp_box_team.csv", header=True, inferSchema=True
    )
    player_expected_assists = spark.read.csv(
        f"/tmp/player_expected_assists.csv", header=True, inferSchema=True
    )

    # Calculo de asistencias agrupadas por equipo
    assists = player_expected_assists.groupBy("Team").agg(
        sum("Actual Assists").alias("Actual Assists"),
        sum("Expected Assists (xA)").alias("Expected Assists"),
    )

    attack_data = (
        big_chance.alias("bc")
        .join(clean_sheet.alias("cs"), col("bc.Team") == col("cs.Team"))
        .join(effective_clearance.alias("ec"), col("bc.Team") == col("ec.Team"))
        .join(expected_goals.alias("eg"), col("bc.Team") == col("eg.Team"))
        .join(ontarget_scoring_att.alias("osa"), col("bc.Team") == col("osa.Team"))
        .join(penalty_won.alias("pw"), col("bc.Team") == col("pw.Team"))
        .join(possession_won_att_3rd.alias("pwa"), col("bc.Team") == col("pwa.Team"))
        .join(team_goals_per_match.alias("tgpm"), col("bc.Team") == col("tgpm.Team"))
        .join(touches_in_opp_box.alias("tiob"), col("bc.Team") == col("tiob.Team"))
        .join(assists.alias("as"), col("bc.Team") == col("as.Team"))
        .select(
            col("bc.Team"),
            col("bc.Big Chances"),
            col("cs.Clean Sheets"),
            col("ec.Clearances per Match"),
            col("ec.Total Clearances"),
            col("eg.Expected Goals"),
            col("osa.Shots on Target per Match"),
            col("osa.Shot Conversion Rate (%)"),
            col("pw.Penalties Won"),
            col("pw.Conversion Rate (%)").alias("Penalties Conversion Rate (%)"),
            col("pwa.Possession Won Final 3rd per Match"),
            col("pwa.Total Possessions Won"),
            col("tgpm.Goals per Match"),
            col("tgpm.Total Goals Scored"),
            col("tgpm.Matches").alias("Matches"),
            col("tiob.Touches in Opposition Box"),
            col("as.Actual Assists"),
            col("as.Expected Assists"),
        )
    )

    # Calculo de métricas adicionales
    attack_data = (
        attack_data.withColumn(
            "Goal Conversion Rate", expr("`Goals per Match` / `Big Chances`")
        )
        .withColumn("Clearance Efficiency", expr("`Total Clearances` / `Matches`"))
        .withColumn(
            "Possession Effectiveness",
            expr("`Possession Won Final 3rd per Match` / `Touches in Opposition Box`"),
        )
        .withColumn("Penalty Impact", expr("`Penalties Won` / `Total Goals Scored`"))
        .withColumn(
            "Offensive Performance", expr("(`Goals per Match` + `Expected Goals`) / 2")
        )
        .withColumn(
            "Assist to Goal Ratio", expr("`Actual Assists` / `Total Goals Scored`")
        )
        .withColumn(
            "Shooting Efficiency",
            expr("`Shots on Target per Match` * `Shot Conversion Rate (%)` / 100"),
        )
        .withColumn("Clean Sheet Impact", expr("`Clean Sheets` / `Matches`"))
        .withColumn(
            "Chances per Possession",
            expr("`Big Chances` / `Possession Won Final 3rd per Match`"),
        )
        .withColumn(
            "Combined Attack Efficiency",
            expr(
                "(`Big Chances` + `Expected Goals` + `Touches in Opposition Box`) / `Matches`"
            ),
        )
    )

    # Subir resultado transformado a la capa `stage` en S3
    upload_to_stage(attack_data, "attack_data")


# Transformación de la defensa
def transform_defense():
    spark = create_spark_session("Transform Defense Data")
    # Obtener los CSV desde S3
    expected_goals_conceded = spark.read.csv(
        "/tmp/expected_goals_conceded_team.csv", header=True, inferSchema=True
    )
    goals_conceded_per_match = spark.read.csv(
        "/tmp/goals_conceded_team_match.csv", header=True, inferSchema=True
    )
    interceptions = spark.read.csv(
        "/tmp/interception_team.csv", header=True, inferSchema=True
    )
    penalties_conceded = spark.read.csv(
        "/tmp/penalty_conceded_team.csv", header=True, inferSchema=True
    )
    saves = spark.read.csv("/tmp/saves_team.csv", header=True, inferSchema=True)
    tackles = spark.read.csv("/tmp/won_tackle_team.csv", header=True, inferSchema=True)

    # Unimos los DataFrames usando la columna "Team"
    defense_data = (
        expected_goals_conceded.alias("egc")
        .join(goals_conceded_per_match.alias("gcp"), col("egc.Team") == col("gcp.Team"))
        .join(interceptions.alias("int"), col("egc.Team") == col("int.Team"))
        .join(penalties_conceded.alias("pc"), col("egc.Team") == col("pc.Team"))
        .join(saves.alias("sav"), col("egc.Team") == col("sav.Team"))
        .join(tackles.alias("tac"), col("egc.Team") == col("tac.Team"))
        .select(
            col("egc.Team").alias("Team"),
            col("egc.Matches").alias("Matches"),
            col("egc.Expected Goals Conceded"),
            col("gcp.Goals Conceded per Match"),
            col("gcp.Total Goals Conceded"),
            col("int.Interceptions per Match"),
            col("int.Total Interceptions"),
            col("pc.Penalties Conceded"),
            col("pc.Penalty Goals Conceded"),
            col("sav.Saves per Match"),
            col("sav.Total Saves"),
            col("tac.Successful Tackles per Match"),
            col("tac.Tackle Success (%)"),
        )
    )

    # Calculo de métricas adicionales
    defense_data = (
        defense_data.withColumn(
            "Interceptions Efficiency", expr("`Total Interceptions` / `Matches`")
        )
        .withColumn("Goals Conceded Efficiency", expr("`Goals Conceded per Match`"))
        .withColumn(
            "Save Effectiveness", expr("`Total Saves` / `Total Goals Conceded`")
        )
        .withColumn(
            "Penalty Average per Match", expr("`Penalties Conceded` / `Matches`")
        )
        .withColumn(
            "Penalty Impact on Goals",
            expr("`Penalty Goals Conceded` / `Total Goals Conceded`"),
        )
        .withColumn("Saves per Match Ratio", expr("`Saves per Match` / `Matches`"))
        .withColumn(
            "Successful Tackles Average", expr("`Successful Tackles per Match`")
        )
        .withColumn(
            "Conceded vs Interceptions Ratio",
            expr("`Total Goals Conceded` / `Total Interceptions`"),
        )
        .withColumn(
            "Goals Conceded to Saves Ratio",
            expr("`Total Goals Conceded` / `Total Saves`"),
        )
        .withColumn(
            "Interceptions per Penalty Conceded",
            expr("`Total Interceptions` / `Penalties Conceded`"),
        )
    )

    upload_to_stage(defense_data, "defense_data")


# Transformación de la disciplina
def transform_discipline():

    spark = create_spark_session("Transform Discipline")

    fouls_data = spark.read.csv(
        "/tmp/fk_foul_lost_team.csv", header=True, inferSchema=True
    )
    interceptions_data = spark.read.csv(
        "/tmp/interception_team.csv", header=True, inferSchema=True
    )
    cards_data = spark.read.csv(
        "/tmp/total_yel_card_team.csv", header=True, inferSchema=True
    )

    # Unimos los DataFrames usando la columna "Team"
    discipline_data = (
        fouls_data.alias("fouls")
        .join(interceptions_data.alias("inter"), col("fouls.Team") == col("inter.Team"))
        .join(cards_data.alias("cards"), col("fouls.Team") == col("cards.Team"))
        .select(
            col("fouls.Team").alias("Team"),
            col("fouls.Matches").alias("Matches"),
            col("fouls.Fouls per Match"),
            col("inter.Interceptions per Match"),
            col("inter.Total Interceptions"),
            col("cards.Yellow Cards"),
            col("cards.Red Cards"),
        )
    )

    # Calculo de métricas adicionales
    discipline_data = (
        discipline_data.withColumn(
            "Interceptions Efficiency", expr("`Total Interceptions` / `Matches`")
        )
        .withColumn(
            "Fouls to Interceptions Ratio",
            expr("`Fouls per Match` / `Interceptions per Match`"),
        )
        .withColumn("Yellow Cards per Match", expr("`Yellow Cards` / `Matches`"))
        .withColumn("Red Cards per Match", expr("`Red Cards` / `Matches`"))
        .withColumn(
            "Fouls per Yellow Card",
            expr("(`Fouls per Match` * `Matches`) / `Yellow Cards`"),
        )
        .withColumn(
            "Interceptions per Card",
            expr("`Total Interceptions` / (`Yellow Cards` + `Red Cards`)"),
        )
        .withColumn(
            "Cards per Match", expr("(`Yellow Cards` + `Red Cards`) / `Matches`")
        )
        .withColumn("Yellow to Red Cards Ratio", expr("`Yellow Cards` / `Red Cards`"))
        .withColumn(
            "Discipline Index",
            expr(
                "(`Yellow Cards` * 1 + `Red Cards` * 2 + `Fouls per Match` * `Matches`) / `Matches`"
            ),
        )
        .withColumn(
            "Interceptions Impact",
            expr("`Total Interceptions` / (`Fouls per Match` * `Matches`)"),
        )
    )

    upload_to_stage(discipline_data, "discipline_data")


# Crear archivo de estado `_SUCCESS` o `_ERROR` después de las transformaciones
def create_stage_status_file():
    s3_hook = S3Hook(aws_conn_id="aws_s3_datalake_user")
    status_key_prefix = f"stage/{source}/status/year={year}/month={month}/day={day}/"
    status_file_key = f"{status_key_prefix}transform_SUCCESS"

    # Crear archivo de estado temporal
    status_file_path = f"/tmp/{os.path.basename(status_file_key)}"
    with open(status_file_path, "w") as status_file:
        status_file.write("Proceso de transformación finalizado con éxito.\n")

    # Subir el archivo de estado a S3
    logger.info(f"Subiendo archivo de estado '{status_file_key}' a S3...")
    s3_hook.load_file(
        filename=status_file_path,
        key=status_file_key,
        bucket_name=bucket_name,
        replace=True,
    )
    if os.path.exists(status_file_path):
        os.remove(status_file_path)


# Definir DAG de Transformaciones
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "b_Transform_football_analytics_transform_to_refined",
    default_args=default_args,
    description="DAG para verificar, transformar y cargar datos en S3 stage",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "data-pipeline", "football-analytics"],
) as dag:

    # Verificar que todos los estados sean `_SUCCESS`
    verify_status_task = ShortCircuitOperator(
        task_id="verify_all_success",
        python_callable=verify_all_success,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Descargar las tablas desde S3
    download_tables_task = PythonOperator(
        task_id="download_tables",
        python_callable=download_tables,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Crear tareas de transformación usando TaskGroup
    with TaskGroup(group_id="transformations") as transformations_group:
        transform_attack_task = PythonOperator(
            task_id="transform_attack",
            python_callable=transform_attack,
            retries=1,
            retry_delay=timedelta(minutes=1),
        )

        transform_defense_task = PythonOperator(
            task_id="transform_defense",
            python_callable=transform_defense,
            retries=1,
            retry_delay=timedelta(minutes=1),
        )

        transform_defense_task = PythonOperator(
            task_id="transform_discipline",
            python_callable=transform_discipline,
            retries=1,
            retry_delay=timedelta(minutes=1),
        )

    # Crear archivo de estado al finalizar las transformaciones
    create_status_file_task = PythonOperator(
        task_id="create_stage_status_file",
        python_callable=create_stage_status_file,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Tarea para disparar el DAG de load
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="c_Load_football_analytics_load_to_exploration",  # Nombre del DAG que quieres disparar
        wait_for_completion=False,  # Esperar a que el DAG finalice (opcional)
    )

    # Definir la secuencia de tareas
    (
        verify_status_task
        >> download_tables_task
        >> transformations_group
        >> create_status_file_task
        >> trigger_load_dag
    )
