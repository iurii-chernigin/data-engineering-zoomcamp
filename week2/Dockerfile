FROM prefecthq/prefect:2.7.7-python3.9

COPY prefect-docker/docker-requirments.txt .
RUN pip install -r docker-requirments.txt --trusted-host pypi.python.org --no-cache-dir

COPY prefect-flows/*.py /opt/prefect/flows/
RUN mkdir -p /opt/prefect/yellow /opt/prefect/green
