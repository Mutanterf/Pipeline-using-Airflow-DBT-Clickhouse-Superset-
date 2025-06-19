from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import os
from clickhouse_driver import Client

# Подключения
POSTGRES_CONN = "postgresql+psycopg2://postgres:Mika2u7w@host.docker.internal:5432/data_filter"
CLICK_PARAMS = dict(host='clickhouse', user='nit_data', password='Mika2u7w', database='default')
DATA_DIR = '/opt/airflow/data' 

def load_all_to_clickhouse():
    engine = create_engine(POSTGRES_CONN)
    client = Client(**CLICK_PARAMS)
    postgres_tables = ['cks', 'cks_long']

    for table_name in postgres_tables:
        print(f"Загружаем таблицу {table_name} из PostgreSQL...")
        df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine)
        df = df.fillna('').astype(str)
        cols = ', '.join(f"{col} String" for col in df.columns)
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
        client.execute(f"CREATE TABLE {table_name} ({cols}) ENGINE = MergeTree() ORDER BY tuple()")
        client.execute(f"INSERT INTO {table_name} VALUES", list(df.to_records(index=False)))
        print(f"Загружено {len(df)} строк в {table_name}.")

    xlsx_files = sorted([f for f in os.listdir(DATA_DIR) if f.lower().endswith('.xlsx')])[:6]
    for file in xlsx_files:
        file_path = os.path.join(DATA_DIR, file)
        df = pd.read_excel(file_path)
        df = df.fillna('').astype(str)

        table_name = os.path.splitext(file)[0].lower()
        cols = ', '.join(f"{col} String" for col in df.columns)

        client.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols}) ENGINE = MergeTree() ORDER BY tuple()")
        client.execute(f"INSERT INTO {table_name} VALUES", list(df.to_records(index=False)))
        print(f"завершено {file} как таблица {table_name}.")

def convert_types_in_clickhouse():
    client = Client(**CLICK_PARAMS)

    # Этап 1: создание таблиц с нужными типами
    creation_queries = [
        "DROP TABLE IF EXISTS coefficient_rozhdaemosti_typed",
        """
        CREATE TABLE coefficient_rozhdaemosti_typed (
            CSI_NAME String,
            SPACE_NAME String,
            RYEAR UInt16,
            PERIOD_NAME String,
            VALUE Float64,
            VALUE_MEASURE String,
            SPR1 String,
            SPR2 String,
            CUBE_SET_ID UInt32,
            SPACE_ELEMENT_SET_ID UInt64,
            REPORTING_PERIOD UInt16,
            SPR1_CLASS_VERSION UInt16,
            SPR2_CLASS_VERSION UInt16
        ) ENGINE = MergeTree() ORDER BY tuple()
        """,

        "DROP TABLE IF EXISTS coefficient_smertnosti_typed",
        "CREATE TABLE coefficient_smertnosti_typed AS coefficient_rozhdaemosti_typed",

        "DROP TABLE IF EXISTS estestvenniy_prirost_ubil_2024_typed",
        """
        CREATE TABLE estestvenniy_prirost_ubil_2024_typed (
            CSI_NAME String,
            SPACE_NAME String,
            RYEAR UInt16,
            PERIOD_NAME String,
            VALUE Int32,
            VALUE_MEASURE String,
            SPR1 String,
            SPR2 String,
            SPR3 String,
            CUBE_SET_ID UInt32,
            SPACE_ELEMENT_SET_ID UInt64,
            REPORTING_PERIOD UInt16,
            SPR1_CLASS_VERSION UInt16,
            SPR2_CLASS_VERSION UInt16,
            SPR3_CLASS_VERSION UInt16
        ) ENGINE = MergeTree() ORDER BY tuple()
        """,

        "DROP TABLE IF EXISTS migration_prirost_ubil_typed",
        """
        CREATE TABLE migration_prirost_ubil_typed (
            CSI_NAME String,
            SPACE_NAME String,
            RYEAR UInt16,
            PERIOD_NAME String,
            VALUE Int32,
            VALUE_MEASURE String,
            SPR1 String,
            SPR2 String,
            SPR3 String,
            SPR4 String,
            CUBE_SET_ID UInt32,
            SPACE_ELEMENT_SET_ID UInt64,
            REPORTING_PERIOD UInt16,
            SPR1_CLASS_VERSION UInt16,
            SPR2_CLASS_VERSION UInt16
        ) ENGINE = MergeTree() ORDER BY tuple()
        """,

        "DROP TABLE IF EXISTS obshaya_chislennost_naseleniya_typed",
        """
        CREATE TABLE obshaya_chislennost_naseleniya_typed (
            CSI_NAME String,
            SPACE_NAME String,
            RYEAR UInt16,
            PERIOD_NAME String,
            VALUE Int32,
            VALUE_MEASURE String,
            SPR1 String,
            SPR2 String,
            SPR3 String,
            CUBE_SET_ID UInt32,
            SPACE_ELEMENT_SET_ID UInt64,
            REPORTING_PERIOD UInt16,
            SPR1_CLASS_VERSION UInt16,
            SPR2_CLASS_VERSION UInt16,
            SPR3_CLASS_VERSION UInt16
        ) ENGINE = MergeTree() ORDER BY tuple()
        """,

        "DROP TABLE IF EXISTS sred_prodolzhitelnost_zhizni_typed",
        """
        CREATE TABLE sred_prodolzhitelnost_zhizni_typed (
            CSI_NAME String,
            SPACE_NAME String,
            RYEAR UInt16,
            PERIOD_NAME String,
            VALUE Float64,
            VALUE_MEASURE String,
            SPR1 String,
            SPR2 String,
            SPR3 String,
            CUBE_SET_ID UInt32,
            SPACE_ELEMENT_SET_ID UInt64,
            REPORTING_PERIOD UInt16,
            SPR1_CLASS_VERSION UInt16,
            SPR2_CLASS_VERSION UInt16,
            SPR3_CLASS_VERSION UInt16
        ) ENGINE = MergeTree() ORDER BY tuple()
        """
    ]

    # Этап 2: вставка с преобразованием типов
    insert_queries = [
        """
        INSERT INTO coefficient_rozhdaemosti_typed
        SELECT
            CSI_NAME,
            SPACE_NAME,
            toUInt16OrZero(RYEAR),
            PERIOD_NAME,
            coalesce(toFloat64OrNull(replaceAll(VALUE, ',', '.')), 0.0),
            VALUE_MEASURE,
            SPR1,
            SPR2,
            toUInt32OrZero(CUBE_SET_ID),
            toUInt64OrZero(SPACE_ELEMENT_SET_ID),
            toUInt16OrZero(REPORTING_PERIOD),
            toUInt16OrZero(SPR1_CLASS_VERSION),
            toUInt16OrZero(SPR2_CLASS_VERSION)
        FROM coefficient_rozhdaemosti
        """,

        """
        INSERT INTO coefficient_smertnosti_typed
        SELECT
            CSI_NAME,
            SPACE_NAME,
            toUInt16OrZero(RYEAR),
            PERIOD_NAME,
            coalesce(toFloat64OrNull(replaceAll(VALUE, ',', '.')), 0.0),
            VALUE_MEASURE,
            SPR1,
            SPR2,
            toUInt32OrZero(CUBE_SET_ID),
            toUInt64OrZero(SPACE_ELEMENT_SET_ID),
            toUInt16OrZero(REPORTING_PERIOD),
            toUInt16OrZero(SPR1_CLASS_VERSION),
            toUInt16OrZero(SPR2_CLASS_VERSION)
        FROM coefficient_smertnosti
        """,

        """
        INSERT INTO estestvenniy_prirost_ubil_2024_typed
        SELECT
            CSI_NAME,
            SPACE_NAME,
            toUInt16OrZero(RYEAR),
            PERIOD_NAME,
            toInt32OrZero(VALUE),
            VALUE_MEASURE,
            SPR1,
            SPR2,
            SPR3,
            toUInt32OrZero(CUBE_SET_ID),
            toUInt64OrZero(SPACE_ELEMENT_SET_ID),
            toUInt16OrZero(REPORTING_PERIOD),
            toUInt16OrZero(SPR1_CLASS_VERSION),
            toUInt16OrZero(SPR2_CLASS_VERSION),
            toUInt16OrZero(SPR3_CLASS_VERSION)
        FROM estestvenniy_prirost_ubil_2024
        """,

        """
        INSERT INTO migration_prirost_ubil_typed
        SELECT
            CSI_NAME,
            SPACE_NAME,
            toUInt16OrZero(RYEAR),
            PERIOD_NAME,
            toInt32OrZero(VALUE),
            VALUE_MEASURE,
            SPR1,
            SPR2,
            SPR3,
            SPR4,
            toUInt32OrZero(CUBE_SET_ID),
            toUInt64OrZero(SPACE_ELEMENT_SET_ID),
            toUInt16OrZero(REPORTING_PERIOD),
            toUInt16OrZero(SPR1_CLASS_VERSION),
            toUInt16OrZero(SPR2_CLASS_VERSION)
        FROM migration_prirost_ubil
        """,

        """
        INSERT INTO obshaya_chislennost_naseleniya_typed
        SELECT
            CSI_NAME,
            SPACE_NAME,
            toUInt16OrZero(RYEAR),
            PERIOD_NAME,
            toInt32OrZero(VALUE),
            VALUE_MEASURE,
            SPR1,
            SPR2,
            SPR3,
            toUInt32OrZero(CUBE_SET_ID),
            toUInt64OrZero(SPACE_ELEMENT_SET_ID),
            toUInt16OrZero(REPORTING_PERIOD),
            toUInt16OrZero(SPR1_CLASS_VERSION),
            toUInt16OrZero(SPR2_CLASS_VERSION),
            toUInt16OrZero(SPR3_CLASS_VERSION)
        FROM obshaya_chislennost_naseleniya
        """,

        """
        INSERT INTO sred_prodolzhitelnost_zhizni_typed
        SELECT
            CSI_NAME,
            SPACE_NAME,
            toUInt16OrZero(RYEAR),
            PERIOD_NAME,
            coalesce(toFloat64OrNull(replaceAll(VALUE, ',', '.')), 0.0),
            VALUE_MEASURE,
            SPR1,
            SPR2,
            SPR3,
            toUInt32OrZero(CUBE_SET_ID),
            toUInt64OrZero(SPACE_ELEMENT_SET_ID),
            toUInt16OrZero(REPORTING_PERIOD),
            toUInt16OrZero(SPR1_CLASS_VERSION),
            toUInt16OrZero(SPR2_CLASS_VERSION),
            toUInt16OrZero(SPR3_CLASS_VERSION)
        FROM sred_prodolzhitelnost_zhizni
        """
    ]

    # Этап 3: удаление старых таблиц
    drop_old_tables = [
        "DROP TABLE IF EXISTS coefficient_rozhdaemosti",
        "DROP TABLE IF EXISTS coefficient_smertnosti",
        "DROP TABLE IF EXISTS estestvenniy_prirost_ubil_2024",
        "DROP TABLE IF EXISTS migration_prirost_ubil",
        "DROP TABLE IF EXISTS obshaya_chislennost_naseleniya",
        "DROP TABLE IF EXISTS sred_prodolzhitelnost_zhizni"
    ]

    print("Создание таблиц...")
    for query in creation_queries:
        client.execute(query)
        print("Таблица создана")

    print("Импорт данных...")
    for query in insert_queries:
        client.execute(query)
        print("Данные загружены")

    print("Удаление старых таблиц...")
    for query in drop_old_tables:
        client.execute(query)
        print(f"Удалена: {query.split()[-1]}")


# DAG
with DAG(
    dag_id='load_cks_and_excels_to_clickhouse',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["clickhouse", "postgres", "load"]
) as dag:

    load_task = PythonOperator(
        task_id='load_all_to_clickhouse',
        python_callable=load_all_to_clickhouse
    )

    convert_types_task = PythonOperator(
        task_id='convert_types_in_clickhouse',
        python_callable=convert_types_in_clickhouse
    )

    load_task >> convert_types_task
