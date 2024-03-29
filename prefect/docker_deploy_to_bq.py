from prefect.deployments import Deployment
from etl_gcs_to_bq import etl_parent_flow
from prefect.infrastructure.container import DockerContainer

docker_block = DockerContainer.load("zoomproject-docker")

docker_dep = Deployment.build_from_flow(
    flow = etl_parent_flow,
    name = "docker-eia-bq-flow",
    infrastructure = docker_block,
)

if __name__ == "__main__":
    docker_dep.apply()
