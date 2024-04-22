#
# Автор: Владимир
# 2024
#

# Начало секции hvrfRnVLKcak1Xty. Не удаляйте данную строку!

def extract_cpu(data: str):
    import re
    result = re.search(r'^OK: percent was ([\d.]+) %$', data)
    return float(result.group(1))/100 if result else None

def extract_disk(data: str):
    import re
    result = re.search(r'^OK: used percent was ([\d.]+) %$', data)
    return float(result.group(1))/100 if result else None

def extract_memory(data: str):
    import re
    result = re.search(r'^OK: Memory usage was ([\d.]+) %.*$', data)
    return float(result.group(1))/100 if result else None

def extract_ping_loss(data: str):
    import re
    result = re.search(r'^PING OK - Packet loss = ([\d.]+)%, RTA = [\d.]+ ms$', data)
    return result.group(1) if result else None

def extract_ping_rta(data: str):
    import re
    result = re.search(r'^PING OK - Packet loss = [\d.]+%, RTA = ([\d.]+) ms$', data)
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

# Конец секции hvrfRnVLKcak1Xty. Не удаляйте данную строку!

