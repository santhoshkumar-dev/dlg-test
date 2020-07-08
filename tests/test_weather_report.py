import unittest
import datetime
from pyspark.sql import SparkSession

from uk.co.dlg.weather.analytics.weather_report import WeatherApp


class WeatherReportTest(unittest.TestCase):

    @classmethod
    def create_test_spark_session(cls):
        return SparkSession.builder.appName('test-spark-app').getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_test_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_coldest_day(self):
        input_path = 'resources/parquet'
        expected_data = [(datetime.datetime(2016, 2, 1, 0, 0), 0.1, 'Orkney & Shetland')]
        expected = self.spark.createDataFrame(expected_data, ['ObservationDate', 'ScreenTemperature', 'Region']).collect()

        weather_app = WeatherApp(self.spark, input_path)
        result = weather_app.get_coldest_day().collect()
        self.assertEquals(expected, result)

    def test_get_hottest_day(self):
        input_path = 'resources/parquet'
        expected_data = [(datetime.datetime(2016, 2, 1, 0, 0), 2.8, 'Orkney & Shetland')]
        expected = self.spark.createDataFrame(expected_data, ['ObservationDate', 'ScreenTemperature', 'Region']).collect()

        weather_app = WeatherApp(self.spark, input_path)
        result = weather_app.get_hottest_day().collect()
        self.assertEquals(expected, result)