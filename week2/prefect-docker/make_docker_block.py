from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="iuriichernigin/prefect:etl_nyc_dataset_to_gcp",  # insert your image here
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("etl-nyc-dataset-to-gcp", overwrite=True)