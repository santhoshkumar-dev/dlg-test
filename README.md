**DLG - Data Engineer Test**


**Objective**

Convert the Weather Data into Parquet Format.
The converted data should be queryable to answer the following questions:

1.Which date was the hottest day?

2.What was the temperature on that day?

3.In which region was the hottest day?

**Pre-requisites**

python 3.8, pyspark 3.0.0. 
I have used pycharm editor for development.

**Project Structure**

<code>

        dlg-test/
            ├── datalake
            ├── README.md
            ├── requirements.txt
            ├── src
            ├── tests
            └── venv
</code>

**Implementation Overview**

I have used pyspark to convert csv to parquet and partition the input (time series) weather data by year, month and day.
This structure would enable to make use of parquet's metadata efficiently while executing queries.

<code>

            ├── datalake
                ├── raw
                │   └── weather
                └── staging
                    └── weather
                        └── year=2016
                            ├── month=1
                            │   ├── day=1
                                    .
                                    .
                            │   └── day=n
                            └── month=n
                                ├── day=1
                                    .
                                    .
                                └── day=n

</code>

I have split the conversion of csv to parquet and querying into two modules:
 - uk.co.dlg.weather.converters.csv_to_parquet.py: converts input weather csv to parquet files partitioned by year, month, day.
 - uk.co.dlg.weather.analytics.weather_report.py: example (spark) sql query to get some sample reports

**Testing**

To run locally:

<code>

cd dlg-test

python -m venv venv

pip install -r requirements.txt

python src/uk/co/dlg/weather/converters/csv_to_parquet.py datalake/raw/weather/ datalake/staging/weather/

python src/uk/co/dlg/weather/analytics/weather_report.py datalake/staging/weather/

</code>

Added some unit/integration tests in: tests/test_csv_to_parquet.py

**To do**

- Add more tests, documentation

