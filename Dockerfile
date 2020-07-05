FROM python:3

WORKDIR /usr/src/app

ARG host=localhost
ARG port=9009
ARG mq_host
ARG mq_port
ARG access_token
ARG access_secret
ARG consumer_key
ARG consumer_secret

RUN echo "host: $host" >> ./config.yaml \
    && echo "port: $port" >> ./config.yaml \
    && echo "mq_host: $mq_host" >> ./config.yaml \
    && echo "mq_port: $mq_port" >> ./config.yaml \
    && echo "access_token: $access_token" >> ./config.yaml \
    && echo "access_secret: $access_secret" >> ./config.yaml \
    && echo "consumer_key: $consumer_key" >> ./config.yaml \
    && echo "consumer_secret: $consumer_secret" >> ./config.yaml

COPY database_app.py \
    mq_app.py \
    mq_receive_one.py \
    mq_receive_two.py \
    notebook.ipynb \
    requirements.txt \
    spark_app.py \
    twitter_app.py \
    ./

RUN apt-get update && apt-get install -y \
    default-jdk

RUN pip install -r ./requirements.txt

EXPOSE 5555
