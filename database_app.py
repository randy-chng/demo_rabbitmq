import os
import pandas as pd
import sqlite3

from sqlite3 import Error


def create_connection():

    conn = None

    try:
        print(sqlite3.version)
        conn = sqlite3.connect(
            os.path.join(
                os.getcwd(),
                'pythonsqlite.db'
            )
        )

    except Error as e:
        print(e)

    return conn


def create_table():

    try:
        conn = create_connection()

        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS twitter_words (
                date_time text,
                word text,
                word_count integer    
            )
            """
        )

        c.close()
        conn.close()

    except Error as e:
        print(e)


def query_table(query_tbl_stmt):

    result = []

    try:
        conn = create_connection()

        c = conn.cursor()
        c.execute(query_tbl_stmt)

        result = pd.DataFrame(
            data=[_ for _ in c],
            columns=[_[0] for _ in c.description]
        )

        c.close()
        conn.close()

    except Error as e:
        print(e)

    return result


def insert_table(df):

    try:
        conn = create_connection()

        records = df.to_records(index=False).tolist()

        c = conn.cursor()
        c.executemany(
            'INSERT INTO twitter_words VALUES (?,?,?)',
            records
        )

        c.close()
        conn.commit()
        conn.close()

    except Error as e:
        print(e)
