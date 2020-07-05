import yaml

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql import Row, SparkSession

from database_app import create_table, insert_table


def get_sql_context_instance(sparkConf):

    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()

    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):

    print("----------- %s -----------" % str(time))

    if rdd.isEmpty():

        print('RDD is empty')

    else:

        try:
            # get spark sql singleton context from the current context
            spark = get_sql_context_instance(rdd.context.getConf())

            # convert the RDD to Row RDD
            row_rdd = rdd.map(lambda w: Row(word=w))

            # create a DF from the Row RDD
            words_df = spark.createDataFrame(row_rdd)

            # create a temporary view using the DF
            words_df.createOrReplaceTempView('words')

            # do word count on table using SQL and print it
            word_counts_df = spark.sql(
                """
                select
                    from_unixtime(unix_timestamp()) as date_time,
                    word,
                    count(*) as word_count
                from words
                group by word
                """
            )

            word_counts_df.show(truncate=False)

            # create table if it does not exist
            if not globals()['table_create']:
                create_table()
                globals()['table_create'] = True

            # insert data into table
            insert_table(word_counts_df.toPandas())

        except Exception as e:
            print('Error:', e)


if __name__ == '__main__':

    global table_create
    table_create = False

    # create local StreamingContext with * working thread and batch interval of 20 second
    sc = SparkContext(
        'local[*]',
        'TwitterStream'
    )

    ssc = StreamingContext(sc, 20)

    # read data from port
    with open('config.yaml', 'r') as stream:
        details = yaml.safe_load(stream)

    lines = ssc.socketTextStream(
        details['host'],
        details['port'],
        storageLevel=StorageLevel(False, True, False, False, 1)
    )

    # split each tweet into words
    words = lines.flatMap(lambda line: line.split(' '))

    # do processing for each RDD generated in each interval
    words.foreachRDD(process_rdd)

    # start the streaming computation
    ssc.start()

    # wait for the streaming to finish
    ssc.awaitTermination()
