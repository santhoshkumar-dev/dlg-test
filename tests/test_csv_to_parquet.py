import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.utils import AnalysisException

from uk.co.dlg.weather.converters.csv_to_parquet import enrich_with_date_columns, convert_to_parquet


class WeatherCSVToParquetTest(unittest.TestCase):

    @classmethod
    def create_test_spark_session(cls):
        return SparkSession.builder.appName('test-spark-app').getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_test_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_enrich_with_date_columns_using_timestamp_column(self):
        input_data = [('England', '2016-02-01T00:00:00'),
                      ('Wales', '2016-02-21T00:00:00'),
                      ('Scotland', '2016-02-01T00:00:00')]
        input_df = self.spark.createDataFrame(input_data, ['region', 'timestamp'])

        expected_data = [('England', '2016-02-01T00:00:00', 2016, 2, 1),
                         ('Wales', '2016-02-21T00:00:00', 2016, 2, 21),
                         ('Scotland', '2016-02-01T00:00:00', 2016, 2, 1)]
        expected = self.spark.createDataFrame(expected_data, ['region', 'timestamp', 'year', 'month', 'day']).collect()

        results = enrich_with_date_columns(input_df, 'timestamp').collect()
        self.assertEqual(expected, results)

    def test_enrich_with_date_columns_using_date_column(self):
        input_data = [('England', '2016-02-01'),
                      ('Wales', '2016-02-21'),
                      ('Scotland', '2016-02-01')]
        input_df = self.spark.createDataFrame(input_data, ['region', 'recorded_date'])

        expected_data = [('England', '2016-02-01', 2016, 2, 1),
                         ('Wales', '2016-02-21', 2016, 2, 21),
                         ('Scotland', '2016-02-01', 2016, 2, 1)]
        expected = self.spark.createDataFrame(expected_data,
                                              ['region', 'recorded_date', 'year', 'month', 'day']).collect()

        results = enrich_with_date_columns(input_df, 'recorded_date').collect()
        self.assertEqual(expected, results)

    def test_enrich_with_column_throws_error_with_invalid_column(self):
        input_data = [('England', '2016-02-01'),
                      ('Wales', '2016-02-21'),
                      ('Scotland', '2016-02-01')]
        input_df = self.spark.createDataFrame(input_data, ['region', 'recorded_date'])
        with self.assertRaises(AnalysisException) as error:
            enrich_with_date_columns(input_df, 'timestamp')
        self.assertEqual(True, str(error.exception).startswith(
            '''cannot resolve '`timestamp`' given input columns: [recorded_date, region];;'''))

    def test_convert_csv_to_parquet(self):
        input_path = 'resources/input/sample.csv'
        output_path = 'resources/output'
        time_stamp_column = 'ObservationDate'

        input_df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        expected = input_df. \
            withColumn('year', year(time_stamp_column)). \
            withColumn('month', month(time_stamp_column)). \
            withColumn('day', dayofmonth(time_stamp_column)).collect()

        convert_to_parquet(self.spark, input_path, output_path)
        result = self.spark.read.parquet(output_path).collect()

        self.assertEquals(expected, result)
