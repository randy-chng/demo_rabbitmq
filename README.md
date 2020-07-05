## Overview

This project demonstrates
- streaming of tweets (twitter_app.py) into RabbitMQ
- processing of tweets in RabbitMQ in two ways
    - writing tweets into an object store (mq_receive_one.py)
    - doing a word count on tweets and writing into SQLite (mq_receive_two.py, spark_app.py)

## Installation (Docker)

### RabbitMQ

#### Step 1
Run following command
```
docker run -d --hostname my-rabbit --name some-rabbit --publish 5672:5672 rabbitmq:3
```

### Application

#### Step 1
Git clone project
```
cd ~/ && git clone https://github.com/randy-chng/demo_rabbitmq.git
```

#### Step 2
Build image and provide
- RabbitMQ details [A] and [B]
- Twitter details [W], [X], [Y] and [Z]

For local docker desktop installation, consider following values for RabbitMQ details
- [A] = host.docker.internal
- [B] = 5672
```
cd ~/demo_data_pipeline
docker image build --tag mq_pipeline_i --build-arg mq_host=[A] --build-arg mq_port=[B] --build-arg access_token=[W] --build-arg access_secret=[X] --build-arg consumer_key=[Y] --build-arg consumer_secret=[Z] --file Dockerfile .
```

#### Step 3
Run created image
```
docker run --name mq_pipeline_c --publish 5555:5555 -di mq_pipeline_i
```

#### Step 4
Access created container
```
docker exec -it mq_pipeline_c /bin/bash
```

#### Step 5
Run following commands to start pipeline
```
nohup python -u twitter_app.py > twitter_app_output.log 2>&1 &
nohup python -u mq_receive_one.py > mq_receive_one.log 2>&1 &
nohup python -u mq_receive_two.py > mq_receive_two.log 2>&1 &
nohup python -u spark_app.py > spark_app_output.log 2>&1 &
```

## Query SQLite via Jupyter

For sample output, refer to notebook.ipynb

#### Step 1
Access created container
```
docker exec -it mq_pipeline_c /bin/bash
```

#### Step 2
Run following commands to start jupyterlab
```
jupyter lab --ip=0.0.0.0 --port=5555 --allow-root
```

#### Step 3
Visit provided link using local web browser
- http://127.0.0.1:5555/?token=[SOME RANDOM GENERATED TOKEN VALUE]
