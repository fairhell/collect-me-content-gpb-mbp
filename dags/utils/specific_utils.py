#
# Автор: Владимир
# 2024
#

# Начало секции hvrfRnVLKcak1Xty. Не удаляйте данную строку!

def extract_cpu(data: str):
    import re
    result = re.search(r'Percent was ([\d.]+) %', data)
    return round(float(result.group(1))/100, 4) if result else None

def extract_disk(data: str):
    import re
    result = re.search(r'Used_percent was ([\d.]+) %', data)
    return round(float(result.group(1))/100, 4) if result else None

def extract_memory(data: str):
    import re
    result = re.search(r'Memory usage was ([\d.]+) %', data)
    return round(float(result.group(1))/100, 4) if result else None

def extract_ping_loss(data: str):
    import re
    result = re.search(r'Packet loss = ([\d.]+)%', data)
    return round(float(result.group(1))/100, 4) if result else None

def extract_ping_rta(data: str):
    import re
    result = re.search(r'RTA = ([\d.]+) ms', data)
    return result.group(1) if result else None

def cpu_load_prcnt(data):
    return "cpu_load_prcnt"

def DISK_ROOT_usage_prcnt(data):
    return "DISK_ROOT_usage_prcnt"

def DISK_C_usage_prcnt(data):
    return "DISK_C_usage_prcnt"

def DISK_F_usage_prcnt(data):
    return "DISK_F_usage_prcnt"

def DISK_L_usage_prcnt(data):
    return "DISK_L_usage_prcnt"

def DISK_T_usage_prcnt(data):
    return "DISK_T_usage_prcnt"

def memory_usage_prcnt(data):
    return "memory_usage_prcnt"

def PING_PL(data):
    return "PING_PL"

def PING_RTA(data):
    return "PING_RTA"

def calc_resp_p_delay(data: str):
    import json
    json_dict = json.loads(data)

    result = None
    found = None

    for key_id in json_dict.keys():
        if json_dict.get(key_id) > 350:
            found = key_id
        else:
            if found is not None:
                result = json_dict.get(key_id)
                break
    return result

def calc_resp_p_val(data: str):
    import json
    json_dict = json.loads(data)

    result = None
    found = None

    for key_id in json_dict.keys():
        if json_dict.get(key_id) > 350:
            found = key_id
        else:
            if found is not None:
                result = key_id
                break
    return result

def replace_soar_start_value(options: str, start_value):
    import json
    json_opts = json.loads(options)
    # Меняем стартовое значение (второй элемент списка)
    json_opts[1].update({ 'value': start_value })
    return json.dumps(json_opts)


def soar_preprocess_data(data: list):
    import pandas as pd

    schema = {
        'creation': 'creation_date',
        'closure_date': 'closure_date',
        'updated': 'updated_date',

        'identifier': 'identifier',
        'description': 'description',
        'incident_owner': {
            'id': 'inc_owner_id',
            'name': 'inc_owner_name',
            'uuid': 'inc_owner_uuid',
        },
        'irp_arcdstip': 'irp_dst_ip',
        'irp_arcsrcip': 'irp_src_ip',
        'level': {
            'id': 'level_id',
            'name': 'level_name',
        },
        'status': {
            'id': 'status_id',
            'name': 'status_name',
        },
    }

    json_list = []
    print('___DEBUG 1')
    print(data)

    for item in data:
        plain_json = {}
        print('____DEBUG 2, item', item)

        for schema_key in schema.keys():
            if isinstance(schema_key, dict):
                print('____DEBUG 3, schema_key', shema_key)
                for nested_key in schema_key.keys():
                    plain_json.update({ schema.get(schema_key).get(nested_key): item.get(schema_key).get(nested_key) })
            else:
                print('____DEBUG 4, schema_key', shema_key)
                plain_json.update({ schema.get(schema_key): item.get(schema_key) })

        json_list.append(plain_json)

    return pd.json_normalize(json_list)


def soar_extra_properties(schema: dict, group: str, task: str):
    from utils.check_running import safe_name

    # Для БД пропускаем. Ищем только для REST
    if task == T_SQL_PORTION:
        return 'NULL'

    for item_key in schema.keys():
        group_dict = schema.get(item_key)
        if safe_name(item_key) == group:
            for task_key in group_dict.keys():
                task_dict = group_dict.get(task_key)
                if safe_name(task_key) == task:
                    extra_options = task_dict.get(T_OPTIONS).get(T_REST_PARAMS).get(T_DATA)
                    # Удаляем из логов токен
                    extra_options.pop('token', None)
                    return extra_options

    return 'NULL'


# Конец секции hvrfRnVLKcak1Xty. Не удаляйте данную строку!

