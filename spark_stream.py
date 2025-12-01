import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXIST spark_stream
        WITH replication  = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print('Keyspace created successfuly!')


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXIST spark_stream.created_users (
            id UUID PRIMARY KEY,
            First_Name TEXT,
            Last_Name TEXT,
            Gender TEXT,
            Address TEXT,
            Postcode TEXT,
            Email TEXT,
            Username TEXT,
            DOB TEXT
            Registered_Date TEXT,
            Phone TEXT,
            Picture TEXT)
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting Data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('First_Name')
    last_name = kwargs.get('Last_Name')
    gender = kwargs.get('Gender')
    address = kwargs.get('Address')
    postcode = kwargs.get('Postcode')
    email = kwargs.get('Email')
    username = kwargs.get('Username')
    dob = kwargs.get('DOB')
    registered_date = kwargs.get('Registered_Date')
    phone = kwargs.get('Phone')
    picture = kwargs.get('Picture')

    try:
        session.execute("""
            INSERT INTO spark_stream.created_users(user_id, first_name, last_name, gender, address,
                postcode, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address, postcode,
               email, username, dob, registered_date, phone, picture))
        
        logging.info(f"Data inserted for {first_name} {last_name}")
    
    except Exception as e:
        logging.error(f"could not insert data due to {e}")


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
                .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.14,'
                                                'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
                .config('spark.cassandra.connection.host', 'localhost') \
                .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create a spark session due to expection {e}")

    return s_conn


def connect_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    session = None
    try:
        cluster = Cluster(['localhost'])
        cass_session = cluster.connect()
        return cass_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


if __name__ == '__main__':
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection

        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)


