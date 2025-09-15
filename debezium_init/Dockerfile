FROM python:3.12-slim

WORKDIR /code

COPY ./requirements.txt .

RUN pip install -r ./requirements.txt

COPY init_debezium.py /code/app/

CMD ["python", "/code/app/init_debezium.py"]
#CMD ["tail", "-f", "/dev/null"]