import argparse

from pyspark.sql import SparkSession


class WeatherApp:

    def __init__(self, spark, input_path):
        self.spark = spark
        self.input_path = input_path
        weather = spark.read.parquet(input_path)
        weather.createOrReplaceTempView('weather')

    def get_hottest_day(self):
        return self.spark.sql("SELECT  ObservationDate, ScreenTemperature, Region FROM weather "
                              "WHERE ScreenTemperature = (SELECT MAX(ScreenTemperature) FROM weather)")

    def get_coldest_day(self):
        return self.spark.sql("SELECT ObservationDate, ScreenTemperature, Region FROM weather "
                              "WHERE ScreenTemperature = ("
                              "SELECT MIN(ScreenTemperature) FROM weather WHERE ScreenTemperature != -99.0)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Weather Analytics')
    parser.add_argument('input_path', help='path to weather parquet file')
    args = parser.parse_args()
    session = SparkSession.builder.appName("WeatherApp").getOrCreate()
    weather_app = WeatherApp(session, args.input_path)
    weather_app.get_hottest_day()
    weather_app.get_coldest_day()
