FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org

RUN mkdir -p /opt/data
RUN mkdir -p /opt/prefect
RUN mkdir -p /opt/prefect/flows

COPY prefect /opt/prefect
COPY data /opt/data 