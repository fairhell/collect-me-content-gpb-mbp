"""
Получение данных из БД Oracle (SELECT запрос) порциями, на основе ключа.<br>
<br>
В SELECT запросе можно указать любые колонки для чтения, но важно чтобы потом эти же колонки были прописаны в схеме (в примере ниже - db_data)<br>
В схеме в SELECT  запросе задаем размер порции для чтения  (LIMIT, TOP ..)<br>
Также обязательно указываем конструкцию, чтобы можно было считать верную порцию, например:<br>
.. WHERE contract_date >= '__ NEXT_START_VALUE __' ORDER BY contract_date ASC ..<br>
Здесь __ NEXT_START_VALUE __ будет автоматически заменено на последнее значение из предыдущей порции данных<br>
<br>
_Особенности:_<br>
* Если предыдущие данные уже были добавлены  ранее, они фильтруются.<br>
* Если критерию ключа соответствует несколько записей - они корреткно обрабатываются<br>
* Данные для __ NEXT_START_VALUE __ сохраняются в папке dags/realtime/*<br>
<br>
Задание необходимо выполнять после выполнения задания demo_sales (данные берутся из него)<br>
Расписание - @once<br>
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
from config.config_mbp import CUSTOMER, ORA_CONN_2

from utils.common import start_end_ops, create_tables_op, insert_cleanup_ops
from utils.data_to_sql import db_columns_from_schemas
from utils.db_to_sql import extract_prepare_db_ops
import pendulum

local_tz = pendulum.timezone('Europe/Moscow')

# Основные параметры
DAG_ID = basename(__file__).replace(FILE_PY, '')
START_DATE = datetime.datetime(2024, 1, 1, tzinfo = local_tz)

# Сопоставление колонок в исходной БД и БД для записи
db_data = {
    # Подключение к БД Oracle
    ORA_CONN_2: {
        # Название таблицы - не используется, т.к. мы используем собственный SQL SELECT запрос
        ANY: { 
            T_COLUMNS: {
                'ID' : { 'id': TYPE_STR },
                'CREATION_DATE' : { 'dt_creation_date': TYPE_TIMESTAMP },
                'RESP_PRCNT' : { 'resp_prcnt': TYPE_STR },
                'RESP_MAX' : { 'resp_max': TYPE_INT },
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
"SELECT ID, CREATION_DATE, RESP_PRCNT, RESP_MAX FROM afd_monitoring \
WHERE CREATION_DATE >= '__NEXT_START_VALUE__' \
ORDER BY CREATION_DATE ASC \
LIMIT 50;",
                # Колонка ключа, используется для сохранения __NEXT_START_VALUE__
                T_KEY_COLUMN: 'CREATION_DATE',
                # Стартовое значение ключа, используется в __NEXT_START_VALUE__, если нет сохраненного
                T_START_VALUE: '01-01-2024',
            }
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
            T_DATA: 'not_merge'
        },
        # Добавить колонку времени (когда собрали данные)
        DATETIME_ADD: {
            T_DT_COLUMN: 'dt_collected',
        }
    },
    T_OUTPUT: {
        T_TABLES: {
            't_' + DAG_ID: db_columns_list
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

    # операторы начала и завершения. Ничего не делают, разделяют логически
    start, end = start_end_ops()

    # оператор создания структуры таблиц и представлений
    create_tables = create_tables_op(
        dag_id = DAG_ID, customer = CUSTOMER)

    # оператор получения данных (в данном случае из БД) и подготовки SQL для записи
    extract_data, prepare_sql = extract_prepare_db_ops(
        dag_id = DAG_ID, full_schema = db_schema)

    # оператор записи SQL в БД Репорт!Ми, оператор очистки (при необходимости)
    insert_data, cleanup = insert_cleanup_ops(
        dag_id = DAG_ID)

    # последовательность выполения операторов
    start >> create_tables >> extract_data >> prepare_sql >> insert_data >> cleanup >> end
