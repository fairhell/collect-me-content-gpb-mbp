"""
Получение данных из REST API R-VISION на основе ключа времени.<br>
Особенности - предусмотрены 4 набора запросов. Параметры заданы в схеме SOAR_PARAMS<br>
<br>
_Особенности:_<br>
* В запросе используется стартовое значение __NEXT_START_VALUE__ <br>
* Поскольку параметры запроса вложенные, используется специальная функция подстановки, заданая в T_START_CUSTOM_REPLACE<br>
* Данные для __ NEXT_START_VALUE __ сохраняются в папке dags/realtime/start-values/<br>
* Тип стартового значения - текущее время: T_START_VALUE_TYPE: TYPE_CURRENT_TIME
* Формат стартового времени задан в  T_START_VALUE_TIME_FORMAT: '%Y-%m-%d %H:%M:%S'<br>
* Поскольку REST ответ приходит в виде списка записей, то используется функция предобработки, зааданная в T_EXTRACT_PARAMS/T_PREPROCESS_FUNC<br>
<br>
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
API_TOKEN = Variable.get('Soar_token', default_var = None)
CONN_FROM = 'SOAR_nagios'
CONN_TO = 'PG_events'
SCHEMA = 'soar'


SOAR_PARAMS = {
    "SrcIP_between": [
        { "property": "IRP_arcSrcIP", "operator": "between",     "value": ["10.180.16.28", "10.180.16.36"] },
        { "property": "IRP_arcSrcIP", "operator": "not between", "value": ["10.180.16.44", "10.180.16.51"] },
        { "property": "IRP_arcSrcIP", "operator": "not between", "value": ["10.180.16.54", "10.180.16.58"] },
    ],
    "DstIP_between": [
        { "property": "IRP_arcDstIP", "operator": "between",     "value": ["10.180.16.28", "10.180.16.36"] },
        { "property": "IRP_arcDstIP", "operator": "not between", "value": ["10.180.16.44", "10.180.16.51"] },
        { "property": "IRP_arcDstIP", "operator": "not between", "value": ["10.180.16.54", "10.180.16.58"] },
    ],
    "DstIP_in": [
        { "property": "IRP_arcDstIP", "operator": "in", "value": ["10.180.16.87", "10.180.16.178", "10.180.17.241", "10.180.17.242"] },
    ],
    "SrcIP_in": [
        { "property": "IRP_arcSrcIP", "operator": "in", "value": ["10.180.16.87", "10.180.16.178", "10.180.17.241", "10.180.17.242"] },
    ],
}

# Формируем автоматически схему сбора данных
rest_data = { CONN_FROM: {} }
for request in SOAR_PARAMS.keys():

    filter_params = [
        { "property": "category", "operator": "=",  "value": "Инциденты ИБ" },
        { "property": "status",   "operator": "in", "value": [ "В работе", "Расследование", "Ожидание"] },
        { "property": "level",    "operator": "in", "value": [ "Критичный", "Высокий", "Средний"] },
        { "property": "creation", "operator": ">",  "value": [ "__NEXT_START_VALUE__"] },
    ]

    for param in SOAR_PARAMS.get(request):
        filter_params.append(param)

    request_schema = {
        # запрос
        'status': {
            T_COLUMNS: {
                # Поскольку мы получаем данные из функции препроцессинга, то имена колонок совпадают
                'creation_date' : { 'creation_date': TYPE_TIMESTAMP },
                'closure_date' : { 'closure_date': TYPE_TIMESTAMP },
                'updated_date' : { 'updsted_date': TYPE_TIMESTAMP },
                'identifier' : { 'identifier': TYPE_STR },
                'description' : { 'description': TYPE_STR },
                'inc_owner_id' : { 'inc_owner_id': TYPE_INT },
                'inc_owner_name' : { 'inc_owner_name': TYPE_STR },
                'inc_owner_uuid' : { 'inc_owner_uuid': TYPE_STR },
                'irp_src_ip' : { 'irp_src_ip': TYPE_STR },
                'irp_dst_ip' : { 'irp_dst_ip': TYPE_STR },
                'level_id' : { 'level_id': TYPE_INT },
                'level_name' : { 'level_name': TYPE_STR },
                'status_id' : { 'status_id': TYPE_INT },
                'status_name' : { 'status_name': TYPE_STR },
            },
            T_OPTIONS: {
                T_COLUMNS: T_KEYWORDS,
                T_REST_PARAMS: {
                    T_ENDPOINT: 'api/v2/incidents',
                    T_DATA: {
                        "token": API_TOKEN,
                        "fields": ["identifier", "creation", "description", "level", "status", "updated", "incident_owner", "closure_date", "IRP_arcSrcIP", "IRP_arcDstIP"],
                        "filter": filter_params,
                    },
                    T_RESPONSE_CHECK: lambda response: response.json()["success"] == True,
                    T_RESPONSE_FILTER: lambda response: response.json()["data"],
                },
                # Колонка ключа, используется для сохранения __NEXT_START_VALUE__
                T_KEY_COLUMN: 'filter',
                # Стартовое значение ключа, используется в __NEXT_START_VALUE__, если нет сохраненного
                T_START_VALUE: '2024-04-24T14:22:00',
                # TYPE_CURRENT_TIME | TYPE_VALUE_FROM_DATA
                T_START_VALUE_TYPE: TYPE_CURRENT_TIME,
                # Стандартный формат datetime.strftime , например, '%Y-%m-%d %H:%M:%S'.
                T_START_VALUE_TIME_FORMAT: '%Y-%m-%d %H:%M:%S',
                # Функция особой замены стартового значения (необходимо, если параметры REST запроса сложные или вложенные)
                T_START_CUSTOM_REPLACE: 'replace_soar_start_value',
            },
            T_EXTRACT_PARAMS: {
                T_PREPROCESS_FUNC: 'soar_preprocess_data',
            },
        },
    }

    rest_data[CONN_FROM].update({request: request_schema})


db_columns_list = db_columns_from_schemas([
    [rest_data, SRC_REST_DATA]
])

print(db_columns_list)

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
            T_DT_COLUMN: 'dt_collected',
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
    schedule_interval = '@once',
    start_date = START_DATE,
    dagrun_timeout = datetime.timedelta(minutes=30),
    tags=[ CUSTOMER, 'rest', 'R-vision', 'SOAR', 'portion', 'advanced' ],
) as dag:

    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__

    # операторы начала и завершения. Разделяют логически и проверяют, что предыдущее задание успешно выполнилось
    start, end = start_end_ops(dag_id = DAG_ID)

    # оператор создания структуры таблиц и представлений
    create_tables = create_tables_op(
        dag_id = DAG_ID, customer = CUSTOMER)

    # оператор получения данных (в данном случае из REST API) и подготовки SQL для записи
    extract_data, prepare_sql = extract_prepare_rest_ops(
        dag_id = DAG_ID, full_schema = rest_schema)

    operators = {
        T_BEFORE: create_tables,
        T_AFTER: extract_data
    }

    rest_ops(dag_id = DAG_ID, input_schema = rest_schema[T_INPUT], operators = operators)

    # оператор записи SQL в БД Репорт!Ми, оператор очистки (при необходимости)
    insert_data, cleanup = insert_cleanup_ops(
        dag_id = DAG_ID)

    # последовательность выполения операторов
    start >> create_tables
    extract_data >> prepare_sql >> insert_data >> cleanup >> end
