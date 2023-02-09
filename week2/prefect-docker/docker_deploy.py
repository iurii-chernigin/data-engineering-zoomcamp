##import sys
##sys.path.append('../prefect-flows')

from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer 
from etl_gcs_to_bq import etl_gcs_to_bq_base

docker_block = DockerContainer.load("etl-nyc-dataset-to-gcp")

docker_deployment = Deployment.build_from_flow(
    flow=etl_gcs_to_bq_base,
    name="etl_gcs_to_bq_docker",
    infrastructure=docker_block
)


if __name__ == "__main__":
    docker_deployment.apply()