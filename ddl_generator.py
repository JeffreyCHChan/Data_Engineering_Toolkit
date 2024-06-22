

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, LongType
import json
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

def infer_data_type(value):
    if isinstance(value, str):
        return "STRING"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
      if abs(value) > 2147483647: # max range of Int a 4 byte value
        return "LONG"
      else:
        return "INTEGER"
    elif isinstance(value, float):
        return "DOUBLE"
    elif isinstance(value, list):
        # Assuming all elements in the list have the same type
        if value:
            element_type = infer_data_type(value[0])
            return f"ARRAY<{element_type}>"
        else:
            return "ARRAY<STRING>"  # default to STRING if list is empty
    elif isinstance(value, dict):
        # Nested structure, we can handle this by recursing
        fields = [f"{k} {infer_data_type(v)}" for k, v in value.items()]
        return f"STRUCT<{', '.join(fields)}>"
    else:
        # If the value type is not recognized, default to STRING
        return "STRING"

def create_schema(json_structure):
    fields = [f"{k} {infer_data_type(v)}" for k, v in json_structure.items()]
    return f"{', '.join(fields)}"


# Output:
# STRUCT<id LONG, name STRING, scores ARRAY<INTEGER>, address STRUCT<street STRING, city STRING, state STRING>>

    
    
file_path = "./testing.json"


with open(file_path) as json_data:
    json_content = json.load(json_data)

print(f"\n\n\n\n\n{create_schema(json_content)}\n\n\n")
