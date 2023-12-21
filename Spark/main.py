from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg

from settings import SRC_RESTAURANT_PATH, SRC_WEATHER_PATH, DST_PATH
from utils import fill_null_lat_lng, get_geohash


def get_restaurant_df(spark: SparkSession, src_path: str) -> DataFrame:
    """
    The function reads the restaurant data from csv files,
    fills empty latitude and longitude values,
    computes the geohash based on latitude and longitude values,
    and returns the spark DataFrame
    :param spark: SparkSession
    :param src_path: Path to source restaurant data
    :return: A DataFrame containing processed restaurant data from CSV files
    """
    # Read restaurant data from CSV files
    restaurant_df = spark.read.csv(src_path, header=True, inferSchema=True)
    # Get the data with null latitude and longitude values
    na_restaurant_df = restaurant_df.filter(col('lat').isNull() | col('lng').isNull())
    # Compute lat and lng values for empty data by records' city and country
    na_restaurant_df = (na_restaurant_df
                        .withColumn('lat', fill_null_lat_lng(col('city'), col('country'))[0])
                        .withColumn('lng', fill_null_lat_lng(col('city'), col('country'))[1]))
    # Fill NA values for lat and lng values
    restaurant_df = restaurant_df.join(na_restaurant_df, on='id', how='left').select(
        restaurant_df['id'],
        restaurant_df['franchise_id'],
        restaurant_df['franchise_name'],
        restaurant_df['restaurant_franchise_id'],
        restaurant_df['country'],
        restaurant_df['city'],
        na_restaurant_df['lat'],
        na_restaurant_df['lng']
    )
    # Compute geohash based on latitude and longitude
    restaurant_df = (restaurant_df
                     .withColumn('geohash', get_geohash(col('lat'), col('lng'))))
    return restaurant_df


def get_weather_df(spark: SparkSession, src_path: str) -> DataFrame:
    """
    The function reads weather data from Parquet files,
    computes the geohash based on latitude and longitude values,
    removes duplicates by grouping the data by 'geohash', 'year', 'month', 'day'
    and returns the spark DataFrame
    :param spark: SparkSession
    :param src_path: Path to source restaurant data
    :return: A DataFrame containing processed weather data from Parquet files
    """
    # Read weather data from Parquet files
    weather_df = spark.read.parquet(src_path, inferSchema=True)
    # Compute geohash based on latitude and longitude
    weather_df = weather_df.withColumn('geohash', get_geohash(col('lat'), col('lng')))
    # Reduce similar data by grouping the data
    weather_df = (weather_df
                  .select('avg_tmpr_f', 'avg_tmpr_c', 'year', 'month', 'day', 'geohash')
                  .groupby('geohash', 'year', 'month', 'day')
                  .agg(avg("avg_tmpr_f").alias("avg_tmpr_f"), avg("avg_tmpr_c").alias("avg_tmpr_c")))
    return weather_df


def export_joined_df(restaurant_df: DataFrame, weather_df: DataFrame, dst_path: str) -> None:
    """
    The function joins processed restaurant and weather data DataFrames
    and writes the joined data to Parquet files
    :param restaurant_df: A DataFrame containing processed restaurant data from CSV files
    :param weather_df: A DataFrame containing processed weather data from Parquet files
    :param dst_path: Destination path to write joined data
    """
    # Join the data
    result_df = weather_df.join(restaurant_df, on='geohash', how='left').select(
        weather_df['geohash'],
        restaurant_df['id'].alias('restaurant_id'),
        restaurant_df['franchise_id'],
        restaurant_df['franchise_name'],
        restaurant_df['restaurant_franchise_id'],
        restaurant_df['country'],
        restaurant_df['city'],
        restaurant_df['lat'],
        restaurant_df['lng'],
        weather_df['year'],
        weather_df['month'],
        weather_df['day'],
        weather_df['avg_tmpr_f'],
        weather_df['avg_tmpr_c']
    )
    # Save the result to destination folder
    result_df.write.partitionBy('year', 'month', 'day').mode('overwrite').parquet(dst_path)


def main() -> None:
    # Create Spark Session
    spark = (SparkSession.builder
             .master('local[*]')
             .appName('Spark HW')
             .config('spark.driver.memory', '5g')
             .config("spark.sql.execution.arrow.pyspark.enabled", True)
             .config('spark.sql.shuffle.partitions', 4)
             .getOrCreate())

    restaurant_df = get_restaurant_df(spark, str(SRC_RESTAURANT_PATH))

    weather_df = get_weather_df(spark, str(SRC_WEATHER_PATH))

    export_joined_df(restaurant_df, weather_df, str(DST_PATH))


if __name__ == '__main__':
    main()
