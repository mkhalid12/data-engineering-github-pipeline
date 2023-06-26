import json


def read_json_file(path_to_file: str) -> dict:
    with open(path_to_file) as json_file:
        data = json.load(json_file)
    return data


def get_config_value(identifier: str, key: str) -> str:
    """
       Args:
           identifier:
           key:
       """

    data = read_json_file("config.json")
    print(data)
    return_value = data[identifier].get(key)

    assert return_value, f"Value missing for identifier {identifier} and {key}. Please add in config.json!"
    return return_value


def get_table_name(job_name, checkpoint) -> str:
    """ get table name based on config  """

    table_name = get_config_value('JOBS', job_name)['SINK_TABLE']
    is_partition_table = eval(get_config_value('JOBS', job_name)['CREATE_PARTITION_TABLE'])
    if is_partition_table:
        partition_table = f"{table_name}_{checkpoint.replace('-', '')}"
        return partition_table
    return table_name
