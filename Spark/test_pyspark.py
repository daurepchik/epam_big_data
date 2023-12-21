from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, LongType
from pyspark.testing.utils import assertSchemaEqual

from settings import SRC_RESTAURANT_PATH, SRC_WEATHER_PATH

spark = (SparkSession.builder
         .master('local[*]')
         .appName('Spark HW')
         .config('spark.driver.memory', '5g')
         .config('spark.executor.memory', '5g')
         .config("spark.sql.execution.arrow.pyspark.enabled", True)
         .config('spark.sql.shuffle.partitions', 4)
         .getOrCreate())

restaurant_df = spark.read.csv(str(SRC_RESTAURANT_PATH), header=True, inferSchema=True)
weather_df = spark.read.parquet(str(SRC_WEATHER_PATH), inferSchema=True)

restaurant_schema = restaurant_df.schema
restaurant_schema_check = StructType([
    StructField('id', LongType(), True),
    StructField('franchise_id', IntegerType(), True),
    StructField('franchise_name', StringType(), True),
    StructField('restaurant_franchise_id', IntegerType(), True),
    StructField('country', StringType(), True),
    StructField('city', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lng', DoubleType(), True)
])

assertSchemaEqual(restaurant_schema, restaurant_schema_check)

weather_schema = weather_df.schema
weather_schema_check = StructType([
    StructField('lng', DoubleType(), True),
    StructField('lat', DoubleType(), True),
    StructField('avg_tmpr_f', DoubleType(), True),
    StructField('avg_tmpr_c', DoubleType(), True),
    StructField('wthr_date', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('day', IntegerType(), True)
])

assertSchemaEqual(weather_schema, weather_schema_check)

print('Tests passed')
