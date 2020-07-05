import yaml
import socket

from mq_app import mq_receive_twitter_feed


def callback(ch, method, properties, body):

    print('[x]', body.decode())

    conn.send(str.encode(body.decode() + '\n'))


if __name__ == "__main__":

    with open('config.yaml', 'r') as stream:
        details = yaml.safe_load(stream)

    TCP_IP = details['host']
    TCP_PORT = details['port']

    global conn
    conn = None

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)

    print('Waiting for TCP connection...')
    conn, addr = s.accept()

    print('Connected... Starting getting tweets.')

    channel, queue_name = mq_receive_twitter_feed()

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()
