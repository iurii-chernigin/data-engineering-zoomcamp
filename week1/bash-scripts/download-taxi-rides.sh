#! /bin/bash

mkdir ../jupyter-notebooks/ny_taxi_raw_data
wget -P ../jupyter-notebooks/ny_taxi_raw_data https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
gzip -d ../jupyter-notebooks/ny_taxi_raw_data/green_tripdata_2019-01.csv.gz