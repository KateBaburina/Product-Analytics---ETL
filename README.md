## Product Analytics - ETL

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Apache](https://img.shields.io/badge/apache-%23D42029.svg?style=for-the-badge&logo=apache&logoColor=white)
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![NumPy](https://img.shields.io/badge/numpy-%23013243.svg?style=for-the-badge&logo=numpy&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)

## Name
Product Analytics - ETL

## Description
We have a service that consists of the social network and the messenger. We are monitoring product metrics for both services (DAU, MAU, WAU, CTR, Retention, messages sent/received). ETL is a pipeline consisting of Extract, Tranform, Load. We are extracting the data from DB, making some transformations - creating slices, then loading transformed data to the table in Clikhouse every day, appending the table. The process is automated to run every day at 11 am.

## Tools used

## Automation

Apache Airflow, dags

## DB

ClickHouse

## Task

We need to create an ETL pipeline that is extracting the data from DB, making some transformations - creating slices, then loading transformed data to the table in Clikhouse every day, appending the table. The process is automated to run every day at 11 am.
