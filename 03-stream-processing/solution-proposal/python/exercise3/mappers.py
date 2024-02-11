import json
from typing import Dict

import pandas
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import MapType, StringType


@pandas_udf(MapType(StringType(), StringType()))
def get_visited_page(input: pandas.Series) -> pandas.Series:
    values_to_clean_as_dict = input.to_dict()
    values_to_return = {}
    for index, json_to_clean in values_to_clean_as_dict.items():
        json_dict = json.loads(json_to_clean)
        try:
            values_to_return[index] = {'value': json.dumps({'page': json_dict["visitedPage"]}), 'topic': 'valid_data'}
        except Exception:
            values_to_return[index] = {'value': json_to_clean, 'topic': 'dead_letter'}
    return pandas.Series(values_to_return)
