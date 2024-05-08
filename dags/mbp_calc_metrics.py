"""
Регулярный расчет метрик на уровне процедур Postgres в слое DDS
<br>
Расписание - каждые 5 минут (*/5 * * * *)<br>
"""
#
# 2024
#
import datetime
import copy
from os.path import basename
from airflow import DAG

# импортируем переменные и параметры подключений из конфиг файла
from config.const import *
from config.common import DEFAULT_DAG_ARGS
from config.config_mbp import CUSTOMER

from utils.common import start_end_ops, create_tables_op
import pendulum
import json

local_tz = pendulum.timezone('Europe/Moscow')

# Основные параметры
DAG_ID = basename(__file__).replace(FILE_PY, '')
START_DATE = datetime.datetime(2023, 1, 1, tzinfo = local_tz)
CONN_TO = 'PG_events_soar'

with DAG(
    dag_id = DAG_ID,
    default_args = DEFAULT_DAG_ARGS,
    schedule_interval = '*/5 * * * *',
    start_date = START_DATE,
    dagrun_timeout = datetime.timedelta(minutes=30),
    tags=[ CUSTOMER, 'SQL', 'DDS', 'calculate' ],
) as dag:

    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__

    # операторы начала и завершения. Разделяют логически и проверяют, что предыдущее задание успешно выполнилось
    start, end = start_end_ops(dag_id = DAG_ID)

    # оператор создания структуры таблиц и представлений
    calculate_metrics = create_tables_op(
        dag_id = DAG_ID, customer = CUSTOMER, connId = CONN_TO)

    # последовательность выполения операторов
    start >> calculate_metrics >> end
