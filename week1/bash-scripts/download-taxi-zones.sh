#! /bin/bash

mkdir ../jupyter-notebooks/ny_taxi_raw_data
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O ../jupyter-notebooks/ny_taxi_raw_data/green_tripdate_locations_lookup.csv