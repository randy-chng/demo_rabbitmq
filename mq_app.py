import yaml
import pika


def mq_connection():

    with open('config.yaml', 'r') as stream:
        details = yaml.safe_load(stream)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=details['mq_host'],
            port=details['mq_port']
        )
    )

    return connection


def mq_send_twitter_feed(message):

    connection = mq_connection()

    channel = connection.channel()

    channel.exchange_declare(
        exchange='twitter_feed',
        exchange_type='fanout'
    )

    channel.basic_publish(
        exchange='twitter_feed',
        routing_key='',
        body=message
    )

    print("[x] Sent %r" % message)
    print("------------------------------------------")

    connection.close()


def mq_receive_twitter_feed():

    connection = mq_connection()

    channel = connection.channel()

    channel.exchange_declare(
        exchange='twitter_feed',
        exchange_type='fanout'
    )

    result = channel.queue_declare(
        queue='',
        exclusive=True
    )

    queue_name = result.method.queue

    channel.queue_bind(
        exchange='twitter_feed',
        queue=queue_name
    )

    print('[*] Waiting for logs. To exit press CTRL+C')

    return channel, queue_name
