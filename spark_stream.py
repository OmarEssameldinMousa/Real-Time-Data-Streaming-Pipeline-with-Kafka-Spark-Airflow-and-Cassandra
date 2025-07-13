import logging 
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster  
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time
# Configure logging
logging.basicConfig(level=logging.INFO)


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def connect_to_kafka(spark_conn):
    max_retries = 10
    retry_wait_seconds = 5

    for attempt in range(max_retries):
        try:
            logging.info(f"Attempt {attempt + 1} to connect to Kafka...")
            spark_df = spark_conn.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'broker:29092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
            
            logging.info("Kafka dataframe created successfully!")
            return spark_df # On success, return the dataframe immediately

        except Exception as e:
            logging.warning(f"Connection failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_wait_seconds} seconds...")
                time.sleep(retry_wait_seconds)
            else:
                logging.error("Could not connect to Kafka after all retries. Aborting.")
                return None # Return None after all retries have failed

def create_spark_connection():
    try:
        s_conn = (
            SparkSession.builder
            .appName('SparkDataStreaming')
            .master('spark://spark-master:7077')
            .config(
                'spark.jars.packages',
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )
            .config('spark.cassandra.connection.host', 'cassandra_db')
            .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions')
            .config('spark.driver.host', 'spark-submitter')
            .getOrCreate()
        )

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(contact_points=['cassandra_db'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Couldn't create the cassandra session due to exception {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") \
        .filter(col("id").isNotNull())
    print(sel)

    return sel

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()  # Initialize Spark session

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)  # Read streaming data from Kafka topic
        print(f"spark_df: {spark_df}")  # Debugging print statement
        selection_df = create_selection_df_from_kafka(spark_df)  # Parse and select relevant fields from Kafka data
        session = create_cassandra_connection()  # Connect to Cassandra database

        if session is not None:
            create_keyspace(session)  # Create keyspace in Cassandra if it doesn't exist
            create_table(session)     # Create table in Cassandra if it doesn't exist

            logging.info("Streaming is being started...")  # Log streaming start

            # Write streaming DataFrame to Cassandra table
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')  # Set checkpoint directory for fault tolerance
                                .option('keyspace', 'spark_streams')              # Specify Cassandra keyspace
                                .option('table', 'created_users')                 # Specify Cassandra table
                                .start())                                         # Start the streaming query

            streaming_query.awaitTermination()  # Wait for the streaming query to finish
