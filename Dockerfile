FROM quay.io/keboola/docker-custom-python:latest

COPY requirements.txt /requirements.txt

RUN python -u -m pip install -r /requirements.txt

COPY src/ /code/
WORKDIR /data/
CMD ["python", "-u", "/code/main.py"]
