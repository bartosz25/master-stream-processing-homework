import json

import pandas
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StringType, MapType

from cleansing.cleansing_functions import Cleanser


@pandas_udf(MapType(StringType(), StringType()))
def clean_value(values_to_clean: pandas.Series) -> pandas.Series:
    values_to_clean_as_dict = values_to_clean.to_dict()
    values_to_return = {}
    for index, json_to_clean in values_to_clean_as_dict.items():
        json_value = json.loads(json_to_clean)
        json_value['source'] = Cleanser.cleanse_source(json_value)
        json_value['technical'] = Cleanser.cleanse_technical(json_value['technical'])
        topic = 'invalid_data' if json_value['user_id'] == 0 or not json_value['visit_id'] else 'valid_data'
        values_to_return[index] = {'payload': json.dumps(json_value),
                                   'device': json_value['technical']['device']['type'],
                                   'topic': topic}
    return pandas.Series(values_to_return)


def enrich_output_with_full_device_name(input: pandas.DataFrame) -> pandas.DataFrame:
    def combine_with_full_name(value_json: str, device_full_name: str) -> str:
        json_dict = json.loads(value_json)
        json_dict['technical']['device']['full_name'] = device_full_name
        return json.dumps(json_dict)

    for rows in input:
        rows['value'] = rows.apply(lambda single_row: combine_with_full_name(
            single_row['value'], single_row['full_name']), axis=1
                        )
        yield rows
