from pyspark.sql import SparkSession
from pyspark.sql.types import *
from faker import Faker
import random

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FakeDataGenerator") \
    .getOrCreate()

# Initialize Faker
fake = Faker()

# Function to parse DDL to Spark schema
def parse_ddl_to_schema(ddl):
    schema = []
    for line in ddl.strip().split('\n'):
        name, dtype = line.strip().split()
        dtype = dtype.lower()
        if dtype == 'string':
            schema.append(StructField(name, StringType(), True))
        elif dtype == 'int':
            schema.append(StructField(name, IntegerType(), True))
        elif dtype == 'float':
            schema.append(StructField(name, FloatType(), True))
        elif dtype == 'boolean':
            schema.append(StructField(name, BooleanType(), True))
        elif dtype == 'date':
            schema.append(StructField(name, DateType(), True))
        elif dtype == 'timestamp':
            schema.append(StructField(name, TimestampType(), True))
        elif dtype == 'double':
            schema.append(StructField(name, DoubleType(), True))
        elif dtype == 'long':
            schema.append(StructField(name, LongType(), True))
        elif dtype.startswith('array'):
            elementType = dtype[dtype.index('<') + 1:dtype.index('>')]
            elementType = parse_ddl_to_schema(f"element {elementType}")[0].dataType
            schema.append(StructField(name, ArrayType(elementType), True))
        elif dtype.startswith('struct'):
            subfields = dtype[dtype.index('<') + 1:dtype.index('>')].replace(',', ' ')
            structType = StructType(parse_ddl_to_schema(subfields))
            schema.append(StructField(name, structType, True))
        else:
            raise ValueError(f"Unknown data type: {dtype}")
    return StructType(schema)

# Function to generate fake data based on data type
def generate_fake_data(data_type):
    if isinstance(data_type, StringType):
        return fake.word()
    elif isinstance(data_type, IntegerType):
        return random.randint(0, 100)
    elif isinstance(data_type, FloatType):
        return random.uniform(0.0, 100.0)
    elif isinstance(data_type, BooleanType):
        return random.choice([True, False])
    elif isinstance(data_type, DateType):
        return fake.date_this_century()
    elif isinstance(data_type, TimestampType):
        return fake.date_time_this_century()
    elif isinstance(data_type, DoubleType):
        return random.uniform(0.0, 100.0)
    elif isinstance(data_type, LongType):
        return random.randint(0, 100)
    elif isinstance(data_type, ArrayType):
        return [generate_fake_data(data_type.elementType) for _ in range(random.randint(1, 5))]
    elif isinstance(data_type, StructType):
        return {field.name: generate_fake_data(field.dataType) for field in data_type.fields}
    else:
        return None

# Function to generate fake data for a given schema
def generate_fake_data_for_schema(schema, num_records=10):
    data = []
    for _ in range(num_records):
        record = {}
        for field in schema.fields:
            record[field.name] = generate_fake_data(field.dataType)
        data.append(record)
    return data

