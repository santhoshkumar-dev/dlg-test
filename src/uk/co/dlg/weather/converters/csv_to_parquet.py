import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth


def convert_to_parquet(spark, input_path, output_path):
    """
    Converts csv to parquet partitioned by year, month, day

    :param spark: spark session
    :param input_path: path to the csv files
    :param output_path: path where it writes partitioned parquet files to
    :return:
    """
    weather_df = spark.read.csv(input_path, header=True, inferSchema=True)
    enriched_weather_df = enrich_with_date_columns(weather_df, 'ObservationDate')
    enriched_weather_df.write.mode('overwrite').partitionBy('year', 'month', 'day').parquet(output_path)


def enrich_with_date_columns(input_df, time_stamp_column):
    """
    Enriches input data frame by adding three new columns year, month, day

    :param input_df: input data frame
    :param time_stamp_column: the date/time column name in the input data frame
    :return: new data frame with three new fields to the input_df year, month and year
    """
    return input_df. \
        withColumn('year', year(time_stamp_column)). \
        withColumn('month', month(time_stamp_column)). \
        withColumn('day', dayofmonth(time_stamp_column))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Converts input weather csv to parquet')
    parser.add_argument('input_path', help='path to weather csv file')
    parser.add_argument('output_path', help='path to weather parquet file')
    args = parser.parse_args()
    session = SparkSession.builder.appName("WeatherConverterApp").getOrCreate()
    print(f'Converting csv input {args.input_path} to parquet')
    convert_to_parquet(session, args.input_path, args.output_path)
    print(f'Parquet output written at: {args.output_path}')
