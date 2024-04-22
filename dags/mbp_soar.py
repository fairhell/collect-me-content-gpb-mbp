"""
Получение данных из IRP Security Vision по REST API, на основе токена.<br>
Расписание - каждые 5 минут (*/5 * * * *)<br>
"""
#
# Автор: Владимир
# 2024
#
import datetime
import copy
from os.path import basename
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable

# импортируем переменные и параметры подключений из конфиг файла
from config.const import *
from config.common import DEFAULT_DAG_ARGS
from config.config_mbp import CUSTOMER


from utils.common import start_end_ops, create_tables_op, insert_cleanup_ops
from utils.data_to_sql import db_columns_from_schemas
from utils.db_to_sql import extract_prepare_db_ops
import pendulum

local_tz = pendulum.timezone('Europe/Moscow')

# Основные параметры
DAG_ID = basename(__file__).replace(FILE_PY, '')
START_DATE = datetime.datetime(2024, 1, 1, tzinfo = local_tz)
CONN_FROM = 'REST_soar'
CONN_TO = 'PG_events'
SCHEMA = 'soar'
API_TOKEN = Variable.get('SOAR_token', default_var = None)

with DAG(
    dag_id = DAG_ID,
    default_args = DEFAULT_DAG_ARGS,
    schedule_interval = '*/5 * * * *',
    start_date = START_DATE,
    dagrun_timeout = datetime.timedelta(minutes=30),
    tags=[ CUSTOMER, 'mbp', 'SOAR', 'API', 'REST', 'IB' ],
) as dag:

    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__

    # операторы начала и завершения. Разделяют логически и проверяют, что предыдущее задание успешно выполнилось
    start, end = start_end_ops(dag_id = DAG_ID)

    # оператор создания структуры таблиц и представлений
    create_tables = create_tables_op(
        dag_id = DAG_ID, customer = CUSTOMER, connId = CONN_TO)

    soar_task = HttpOperator(
        task_id = 'soar_task',
        method = 'GET',
        http_conn_id = CONN_FROM,
        endpoint = 'api/v2/incidents',
        data = { "token": API_TOKEN, "fields": ["identifier"], "filter": [{"in:Cpu,Memory,PING,Disk"}] },
        headers= { "Content-Type": "application/json" },
    )

    # последовательность выполения операторов
    start >> soar_task >> end
