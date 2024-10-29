import mysql.connector
import os
from mysql.connector import Error
import logging
import datetime

db_config = {
    "user": "root",
    "password": os.getenv("MYSQL_ROOT_PASSWORD", "password"),
    "host": os.getenv("MYSQL_HOST", "mysql"),
}

db_name = os.getenv("MYSQL_DATABASE", "schedulerdb")


def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"Database '{db_name}' checked/created.")


def create_metrics_table(cursor):
    table_name = "scheduler_metrics"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        latency FLOAT,
        cpu_load FLOAT,
        throughput FLOAT,
        framework ENUM('SF', 'SL') NOT NULL
    )
    """
    cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' checked/created.")


def create_framework_start_times_table(cursor):
    start_times_table_name = "framework_start_times"
    """Create a table for storing framework start times, if it doesn't exist."""
    create_start_times_table_query = f"""
    CREATE TABLE IF NOT EXISTS {start_times_table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        framework ENUM('SF', 'SL') NOT NULL
    )
    """
    cursor.execute(create_start_times_table_query)
    print(f"Table '{start_times_table_name}' checked/created.")


def init_database():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        create_database(cursor, db_name)
        conn.database = db_name
        create_metrics_table(cursor)
        create_framework_start_times_table(cursor)
        logging.info("Database and table initialized successfully.")

    except Error as e:
        logging.error(f"Error during database initialization: {e}")

    finally:
        cursor.close()
        conn.close()


def insert_scheduler_metrics(timestamp:datetime, latency:float, cpu_load:float, throughput:float, framework:str):
    table_name = "scheduler_metrics"
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()

        insert_query = f"""
        INSERT INTO {table_name} (timestamp, latency, cpu_load, throughput, framework)
        VALUES (%s, %s, %s, %s, %s)
        """
        data = (timestamp, latency, cpu_load, throughput, framework)

        cursor.execute(insert_query, data)
        conn.commit()
    except Error as e:
        logging.error(f"Error inserting data: {e}")

    finally:
        cursor.close()
        conn.close()


def store_decision_in_db(timestamp:datetime, decision:str):
    start_times_table_name = "framework_start_times"
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()

        insert_query = f"""
        INSERT INTO {start_times_table_name} (timestamp, framework)
        VALUES (%s, %s)
        """
        data = (timestamp, decision)

        cursor.execute(insert_query, data)
        conn.commit()
        print("Framework start time inserted successfully.")

    except Error as e:
        print(f"Error inserting framework start time: {e}")

    finally:
        # Clean up
        cursor.close()
        conn.close()
