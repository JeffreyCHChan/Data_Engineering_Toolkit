from pyspark.sql.types import *
from pyspark.sql import SparkSession
from faker import Faker
import random

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FakeDataGenerator") \
    .getOrCreate()

# Initialize Faker
fake = Faker()

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

schema = StructType([StructField('orderId', StringType(), True), StructField('orderDate', StringType(), True), StructField('customer', StructType([StructField('customerId', StringType(), True), StructField('name', StringType(), True), StructField('email', StringType(), True), StructField('phone', StringType(), True), StructField('billingAddress', StructType([StructField('street', StringType(), True), StructField('city', StringType(), True), StructField('state', StringType(), True), StructField('zipCode', StringType(), True), StructField('country', StringType(), True)]), True), StructField('shippingAddress', StructType([StructField('street', StringType(), True), StructField('city', StringType(), True), StructField('state', StringType(), True), StructField('zipCode', StringType(), True), StructField('country', StringType(), True)]), True)]), True), StructField('items', ArrayType(StructType([StructField('itemId', StringType(), True), StructField('productName', StringType(), True), StructField('quantity', IntegerType(), True), StructField('unitPrice', DoubleType(), True), StructField('totalPrice', DoubleType(), True), StructField('attributes', StructType([StructField('color', StringType(), True), StructField('warranty', StringType(), True)]), True)]), True), True), StructField('payment', StructType([StructField('method', StringType(), True), StructField('transactionId', StringType(), True), StructField('amount', DoubleType(), True), StructField('currency', StringType(), True), StructField('status', StringType(), True), StructField('billingAddress', StructType([StructField('street', StringType(), True), StructField('city', StringType(), True), StructField('state', StringType(), True), StructField('zipCode', StringType(), True), StructField('country', StringType(), True)]), True)]), True), StructField('shipping', StructType([StructField('method', StringType(), True), StructField('cost', DoubleType(), True), StructField('carrier', StringType(), True), StructField('trackingNumber', StringType(), True), StructField('estimatedDelivery', StringType(), True), StructField('status', StringType(), True)]), True), StructField('discounts', ArrayType(StructType([StructField('code', StringType(), True), StructField('amount', DoubleType(), True), StructField('description', StringType(), True)]), True), True), StructField('orderTotal', StructType([StructField('subtotal', DoubleType(), True), StructField('discount', DoubleType(), True), StructField('shipping', DoubleType(), True), StructField('tax', DoubleType(), True), StructField('grandTotal', DoubleType(), True)]), True), StructField('notes', StringType(), True)])


fake_data = generate_fake_data_for_schema(schema, num_records=1)

print(fake_data)