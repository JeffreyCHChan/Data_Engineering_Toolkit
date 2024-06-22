

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, LongType
import json
from pyspark.sql import SparkSession

def infer_data_type(value):
    if isinstance(value, str):
        return StringType()
    elif isinstance(value, int):
      if abs(value) > 2147483647: # max range of Int a 4 byte value
        return LongType()
      else:
        return IntegerType()
    elif isinstance(value, float):
        return DoubleType()
    elif isinstance(value, bool):
        return BooleanType()
    elif isinstance(value, list):
        # Assuming all elements in the list have the same type
        if value:
            element_type = infer_data_type(value[0])
            return ArrayType(element_type)
        else:
            return ArrayType(StringType())  # default to StringType if list is empty
    elif isinstance(value, dict):
        # Nested structure, we can handle this by recursing
        return StructType([StructField(k, infer_data_type(v), True) for k, v in value.items()])
    else:
        # If the value type is not recognized, default to StringType
        return StringType()
    
    
def create_schema(json_structure):
    fields = [StructField(k, infer_data_type(v), True) for k, v in json_structure.items()]
    return StructType(fields)

    
    
file_path = "C:\\Users\\206771\\OneDrive - CN\Primary\\206771\\Dev Tools\\helper\\testing.json"


with open(file_path) as json_data:
    json_content = json.load(json_data)

print(f"\n\n\nfrom pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, LongType\n\n{create_schema(json_content)}\n\n\n")
