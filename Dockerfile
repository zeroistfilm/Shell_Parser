FROM python:3.11

WORKDIR /Home

RUN pip3 install aiokafka sqlalchemy mysqlclient

COPY . .

CMD ["python3", "consumer.py"]
