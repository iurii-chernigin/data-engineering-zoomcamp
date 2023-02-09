prefect deployment build -n etl_gcs_to_bq_gh -q default -sb github/gh-nyc-taxi-etl --apply week2/prefect-flows/etl_gcs_to_bq.py:etl_gcs_to_bq_base
