"""
Получение данных из БД Oracle (SELECT запрос) порциями, на основе ключа.<br>
<br>
В SELECT запросе указаны колонки для чтения, и эти же колонки прописаны в схеме db_data)<br>
Также обязательно указываем конструкцию, чтобы можно было считать верную порцию, например:<br>
.. WHERE some_column >= '__ NEXT_START_VALUE __' ORDER BY contract_date ASC ..<br>
Здесь __ NEXT_START_VALUE __ будет автоматически заменено на последнее значение из предыдущей порции данных<br>
<br>
_Особенности:_<br>
* Если предыдущие данные уже были добавлены  ранее, они фильтруются.<br>
* Если критерию ключа соответствует несколько записей - они корреткно обрабатываются<br>
* Данные для __ NEXT_START_VALUE __ сохраняются в папке dags/realtime/start-values/<br>
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
CONN_FROM = 'ORA_flrsa_srv'
CONN_TO = 'PG_events_smft'
SCHEMA = 'smft'

# Сопоставление колонок в исходной БД и БД для записи
db_data = {
    # Подключение к БД Oracle
    CONN_FROM: {
        # Название таблицы - не используется, т.к. мы используем собственный SQL SELECT запрос
        ANY: { 
            T_COLUMNS: {
                'ID' : { 'id': TYPE_STR },
                'CREATION_DATE' : { 'creation_date': TYPE_TIMESTAMP },
                'RESP_PRCNT': { 'resp_prcnt': TYPE_STR },
                'RESP_MAX': { 'resp_max': TYPE_FLOAT },
                'HOSTNAME': { 'hostname': TYPE_STR },
                'SERVERNAME': { 'servername': TYPE_STR },
                'TPM_IN': { 'tpm_in': TYPE_INT },
                'TPM_OUT': { 'tpm_out': TYPE_INT },
            },
            T_OPTIONS: {
                T_COLUMNS: T_KEYWORDS,
                T_SQL_DB: T_SQL_DB_ORACLE,
                T_SQL_SELECT:
# Наш собственный SELECT запрос.
# В нем сами выставляем размер порции для чтения  (LIMIT, TOP ..)
# Также обязательно указываем конструкцию, чтобы можно было считать верную порцию, например:
# .. WHERE contract_date >= '__NEXT_START_VALUE__' ORDER BY contract_date ASC ..
# Здесь __NEXT_START_VALUE__ будет автоматически заменено на последнее значение из
# предыдущей порции данных
"SELECT ID, \
to_char(CREATION_DATE, 'YYYY-MM-DD HH24:MI:SS') CREATION_DATE,  \
RESP_PRCNT, RESP_MAX, HOSTNAME, SERVERNAME, TPM_IN, TPM_OUT \
FROM sfd.afd_monitoring \
WHERE CREATION_DATE >= to_date('__NEXT_START_VALUE__','YYYY-MM-DD HH24:MI:SS') \
ORDER BY CREATION_DATE ASC",
                # Колонка ключа, используется для сохранения __NEXT_START_VALUE__
                T_KEY_COLUMN: 'CREATION_DATE',
                # Стартовое значение ключа, используется в __NEXT_START_VALUE__, если нет сохраненного
                T_START_VALUE: '2024-05-02 11:05:40',
                # Текущее время - TYPE_CURRENT_TIME или  TYPE_VALUE_FROM_DATA - данные из ответа
                T_START_VALUE_TYPE: TYPE_VALUE_FROM_DATA,

            },
        }
    }
}

db_columns_list = db_columns_from_schemas([
    [db_data, SRC_DB_DATA]
])


# Массив, задающий файлы входные и выходные, а также колонки для чтения
db_schema = {
    T_INPUT: {
        T_TYPE: TYPE_DB,
        T_DATA: db_data
    },
    T_TRANSFORM: {
        T_OPTIONS: {
            T_DATA: DATA_NOT_MERGE,
        },
        # Добавить колонку времени (когда собрали данные)
        DATETIME_ADD: {
            T_DT_COLUMN: 'date_collected',
        }
    },
    T_OUTPUT: {
        T_TABLES: {
            SCHEMA + '.t_' + DAG_ID: db_columns_list
        }
    }
}

with DAG(
    dag_id = DAG_ID,
    default_args = DEFAULT_DAG_ARGS,
    schedule_interval = '*/5 * * * *',
    start_date = START_DATE,
    dagrun_timeout = datetime.timedelta(minutes=30),
    tags=[ CUSTOMER, 'mbp', 'database', 'select', 'portion', 'oracle' ],
) as dag:

    if hasattr(dag, 'doc_md'):
        dag.doc_md = __doc__

    # операторы начала и завершения. Разделяют логически и проверяют, что предыдущее задание успешно выполнилось
    start, end = start_end_ops(dag_id = DAG_ID)

    # оператор создания структуры таблиц и представлений
    create_tables = create_tables_op(
        dag_id = DAG_ID, customer = CUSTOMER, connId = CONN_TO)

    # оператор получения данных (в данном случае из БД) и подготовки SQL для записи
    extract_data, prepare_sql = extract_prepare_db_ops(
        dag_id = DAG_ID, full_schema = db_schema)

    # оператор записи SQL в БД Репорт!Ми, оператор очистки (при необходимости)
    insert_data, cleanup = insert_cleanup_ops(
        dag_id = DAG_ID,
        connId = CONN_TO)

    # последовательность выполения операторов
    start >> create_tables >> extract_data >> prepare_sql >> insert_data >> cleanup >> end
