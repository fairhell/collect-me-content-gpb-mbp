"""
Получение данных из REST API Nagios по списку заданных хостов.<br>
Особенности - для разных хостов предусмотрены разный набор запросов. Параметры заданы в схеме SERVERS<br>
<br>
_Особенности:_<br>
* Полученные данные обрабатываются согласно правилам, заданным в REST_PARAMS.<br>
* В запросе используется значение согласно request_name <br>
* Ответ преобразуется согласно функции, заданной в metric_name<br>
* Значение метрики выделяется согласно функции, заданной в metric_val<br> 
<br>
Расписание - каждые 5 минут<br>
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
from airflow.models import Variable

# импортируем переменные и параметры подключений из конфиг файла
from config.const import *
from config.common import DEFAULT_DAG_ARGS, PG_BASE_CONN
from config.config_mbp import CUSTOMER

from utils.common import start_end_ops, create_tables_op, insert_cleanup_ops
from utils.data_to_sql import db_columns_from_schemas
from utils.rest_to_sql import rest_ops, extract_prepare_rest_ops
import pendulum

local_tz = pendulum.timezone('Europe/Moscow')

# Основные параметры
DAG_ID = basename(__file__).replace(FILE_PY, '')
START_DATE = datetime.datetime(2023, 1, 1, tzinfo = local_tz)
API_TOKEN = Variable.get('Nagios_token', default_var = None)
CONN_FROM = 'REST_nagios'
CONN_TO = 'PG_events_nagios'
SCHEMA = 'nagios'


SERVERS = {
#    "s-smf-t1ms-n1": [ "CPU", "DISK_C", "DISK_F", "DISK_L", "DISK_T", "MEM", "PING" ],
#    "s-smf-t1ms-n2": [ "CPU", "DISK_C", "DISK_F", "DISK_L", "DISK_T", "MEM", "PING" ],
#    "s-smf-t2ms": [ "CPU", "DISK_C", "MEM", "PING" ],
#    "S-SMF-T1MS-LIST": [ "PING" ],
    "smf-t01asl-n1": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t01asl-n2": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t02asl-n1": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t02asl-n2": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t03asl-n1": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t03asl-n2": [ "CPU", "DISK", "MEM", "PING" ],
#    "ISH-T1IAL-N1": [ "CPU", "DISK", "MEM", "PING" ],
#    "ISH-T1IAL-N2": [ "CPU", "DISK", "MEM", "PING" ],
#    "ISH-T1KFL-N1": [ "CPU", "DISK", "MEM", "PING" ],
#    "ISH-T1KFL-N2": [ "CPU", "DISK", "MEM", "PING" ],
#    "ISH-T1KFL-N3": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t1uil-n1": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t1uil-n2": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t2uil-n1": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t2uil-n2": [ "CPU", "DISK", "MEM", "PING" ],
#    "ISH-T2IAL": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t1orl": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t2orl": [ "CPU", "DISK", "MEM", "PING" ],
    "smf-t3orl": [ "CPU", "DISK", "MEM", "PING" ],
#    "S-SMF-T1UA": [ "CPU", "DISK", "MEM", "PING" ],
#    "SMF-T1WSL-N1": [ "CPU", "DISK", "MEM", "PING" ],
#    "SMF-T04ASL-N1": [ "CPU", "DISK", "MEM", "PING" ],
#    "SMF-T04ASL-N2": [ "CPU", "DISK", "MEM", "PING" ],
}

REST_PARAMS = {
#    "CPU": { "request_name": "Cpu. /I/", "metric_name": "cpu_load_prcnt", "metric_val": "extract_cpu" },
    "CPU": { "request_name": "CPU. /I/", "metric_name": "cpu_load_prcnt", "metric_val": "extract_cpu" },
    "MEM": { "request_name": "Memory /W/", "metric_name": "memory_usage_prcnt", "metric_val": "extract_memory" },
    "DISK": { "request_name": "Disk. / /C/", "metric_name": "DISK_ROOT_usage_prcnt", "metric_val": "extract_disk" },
    "DISK_C": { "request_name": "Disk. C /C/", "metric_name": "DISK_C_usage_prcnt", "metric_val": "extract_disk" },
    "DISK_F": { "request_name": "Disk. F /C/", "metric_name": "DISK_F_usage_prcnt", "metric_val": "extract_disk" },
    "DISK_L": { "request_name": "Disk. L /C/", "metric_name": "DISK_L_usage_prcnt", "metric_val": "extract_disk" },
    "DISK_T": { "request_name": "Disk. T /C/", "metric_name": "DISK_T_usage_prcnt", "metric_val": "extract_disk" },
#    "PING_LOSS": { "request_name": "PING", "metric_name": "PING_PL", "metric_val": "extract_ping_loss" },
    "PING_LOSS": { "request_name": "PING /C/", "metric_name": "PING_PL", "metric_val": "extract_ping_loss" },
#    "PING_RTA": { "request_name": "PING", "metric_name": "PING_RTA", "metric_val": "extract_ping_rta" },
    "PING_RTA": { "request_name": "PING /C/", "metric_name": "PING_RTA", "metric_val": "extract_ping_rta" },
}

def update_schema(
    option_schema: dict,
    option: str,
    host: str):
    option_schema.update({
        # тип запроса, например, GET
        option: {
            T_COLUMNS: {
                # Читаем нужные нам колонки , откуда - куда - тип
                'last_check' : { 'check_time': TYPE_TIMESTAMP },
                'display_name' : { 'display_name': TYPE_STR, 'metric_name': TYPE_STR },
                'host_name': { 'host_name' : TYPE_STR },
                'host_address': { 'host_address': TYPE_STR },
                'output': { 'output': TYPE_STR, 'metric_val': TYPE_FLOAT },
            },
            T_OPTIONS: {
                T_COLUMNS: T_KEYWORDS,
                T_REST_PARAMS: {
                    T_ENDPOINT: 'nagiosxi/api/v1/objects/servicestatus',
                    T_DATA: {
                        "apikey": API_TOKEN,
                        "host_name": host,
                        "display_name": REST_PARAMS[option].get("request_name"),
                        "last_time": "",
                    },
                    T_RESPONSE_CHECK: lambda response: response.json()["recordcount"] > 0,
                    T_RESPONSE_FILTER: lambda response: response.json()["servicestatus"][0],
                },
            },
            T_TRANSFORM: {
                "metric_name": REST_PARAMS[option].get("metric_name"),
                "metric_val": REST_PARAMS[option].get("metric_val"),
            }
        }
    })
    return option_schema


# Формируем автоматически схему сбора данных
rest_data = { CONN_FROM: {} }
for host in SERVERS.keys():
    host_schema = {
        host: {}
    }
    option_schema = {}
    for option in SERVERS.get(host):
        if option == "PING":
            option_schema = update_schema(option_schema, "PING_LOSS", host)
            option_schema = update_schema(option_schema, "PING_RTA", host)
        else:
            option_schema = update_schema(option_schema, option, host)

    rest_data[CONN_FROM].update({host: option_schema})


db_columns_list = db_columns_from_schemas([
    [rest_data, SRC_REST_DATA]
])

# Массив, задающий файлы входные и выходные, а также колонки для чтения
rest_schema = {
    T_INPUT: {
        T_TYPE: TYPE_REST,
        T_DATA: rest_data
    },
    T_TRANSFORM: {
        T_OPTIONS: {
            # DATA_APPEND | DATA_MERGE | DATA_NOT_MERGE
            T_DATA: DATA_APPEND
        },
        # Добавить колонку времени (когда собрали данные)
        DATETIME_ADD: {
            T_DT_COLUMN: 'date_collected',
        }
    },
    T_OUTPUT: {
        T_TABLES: {
            SCHEMA  + '.t_' + DAG_ID: db_columns_list
        }
    }
}

with DAG(
    dag_id = DAG_ID,
    default_args = DEFAULT_DAG_ARGS,
    schedule_interval = '*/5 * * * *',
    start_date = START_DATE,
    dagrun_timeout = datetime.timedelta(minutes=30),
    tags=[ CUSTOMER, 'rest', '', 'nagios', 'portion', 'advanced' ],
    params = {
        T_EXTRA_LOG: True,
        T_LOG_CONID: CONN_TO,
        T_LOG_TABLE: SCHEMA + '.t_' + DAG_ID + '_log',
        T_COLUMN_DATE: 'date_collected',
        T_COLUMN_STATUS: 'is_success',
        T_COLUMN_GROUP: 'host_name',
        T_COLUMN_TASK: 'metric_name',
        T_COLUMN_EXTRA: 'extra',
    }
) as dag:

    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__

    # операторы начала и завершения. Разделяют логически и проверяют, что предыдущее задание успешно выполнилось
    start, end = start_end_ops(dag_id = DAG_ID)

    # оператор создания структуры таблиц и представлений
    create_tables = create_tables_op(
        dag_id = DAG_ID, customer = CUSTOMER, connId = CONN_TO)

    # оператор получения данных (в данном случае из REST API) и подготовки SQL для записи
    extract_data, prepare_sql = extract_prepare_rest_ops(
        dag_id = DAG_ID, full_schema = rest_schema)

    operators = {
        T_BEFORE: create_tables,
        T_AFTER: extract_data,
    }

    rest_ops(dag_id = DAG_ID, input_schema = rest_schema[T_INPUT], operators = operators)

    # оператор записи SQL в БД Репорт!Ми, оператор очистки (при необходимости)
    insert_data, cleanup = insert_cleanup_ops(
        dag_id = DAG_ID,
        connId = CONN_TO)

    # последовательность выполения операторов
    start >> create_tables
    extract_data >> prepare_sql >> insert_data >> cleanup >> end
