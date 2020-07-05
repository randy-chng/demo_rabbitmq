from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from mq_app import mq_receive_twitter_feed


def get_sql_context_instance(spark_conf):

    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=spark_conf) \
            .getOrCreate()

    return globals()['sqlContextSingletonInstance']


def callback(ch, method, properties, body):

    global batching
    batching.append([body.decode()])

    print('[{}]'.format(len(batching)), body.decode())

    if len(batching) == 10:
        readied_batch, batching = batching.copy(), []

        df = spark.createDataFrame(
            readied_batch,
            ['tweet']
        )

        df = df.withColumn(
            'date_time',
            F.lit(datetime.now().strftime('%Y%m%d_%H%M%S'))
        )

        df.show(truncate=False)

        # df.write.parquet(
        #     's3a://some_bucket/tweets',
        #     mode='append',
        #     partitionBy='date_time',
        #     compression='snappy'
        # )


if __name__ == "__main__":

    spark_conf = SparkConf()
    spark_conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_conf.set('spark.hadoop.fs.s3a.access.key', '')
    spark_conf.set('spark.hadoop.fs.s3a.secret.key', '')
    spark_conf.set('spark.hadoop.fs.s3a.endpoint', '')

    spark = get_sql_context_instance(spark_conf)

    global batching
    batching = []

    channel, queue_name = mq_receive_twitter_feed()

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()
