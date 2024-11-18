import mysql.connector
import os
from mysql.connector import Error
import logging
import datetime
import time

db_config = {
    "user": "root",
    "password": os.getenv("MYSQL_ROOT_PASSWORD", "password"),
    "host": os.getenv("MYSQL_HOST", "mysql"),
}

db_name = os.getenv("MYSQL_DATABASE", "schedulerdb")


def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    logging.info(f"Database '{db_name}' checked/created.")


def create_metrics_table(cursor):
    table_name = "scheduler_metrics"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        latency FLOAT,
        cpu_load FLOAT,
        throughput FLOAT,
        input_rate_records_per_second FLOAT,
        framework ENUM('SF', 'SL') NOT NULL
    )
    """
    cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' checked/created.")


def create_framework_start_times_table(cursor):
    start_times_table_name = "framework_start_times"
    create_start_times_table_query = f"""
    CREATE TABLE IF NOT EXISTS {start_times_table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        framework_before ENUM('SF', 'SL') NOT NULL,
        used_framework ENUM('SF', 'SL') NOT NULL,
        u_sf FLOAT,
        u_sl FLOAT,
        going_to_switch BOOLEAN NOT NULL
    )
    """
    cursor.execute(create_start_times_table_query)
    logging.debug(f"Table '{start_times_table_name}' checked/created.")


def create_model_storage_table(cursor):
    model_storage_table_name = "model_storage"
    create_model_storage_table_query = f"""
    CREATE TABLE IF NOT EXISTS {model_storage_table_name} (
        model_name VARCHAR(255) PRIMARY KEY,
        model_data LONGBLOB NOT NULL
    )
    """
    cursor.execute(create_model_storage_table_query)
    logging.info(f"Table '{model_storage_table_name}' checked/created.")


def create_historic_metrics_table(cursor):
    table_name = "historic_metrics"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        latency FLOAT,
        cpu_load FLOAT,
        throughput FLOAT,
        input_rate_records_per_second FLOAT,
        framework ENUM('SF', 'SL') NOT NULL
    )
    """
    cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' checked/created.")


def create_records_processed_table(cursor):
    table_name = "processed_metrics"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME NOT NULL,
        records FLOAT,
        framework ENUM('SF', 'SL') NOT NULL
    )
    """
    cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' checked/created.")


def init_database(debug_Flag=False):
    try:
        if debug_Flag:
            delete_tables()
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        create_database(cursor, db_name)
        conn.database = db_name
        create_metrics_table(cursor)
        create_framework_start_times_table(cursor)
        # create_model_storage_table(cursor)
        create_historic_metrics_table(cursor)
        create_records_processed_table(cursor)
        logging.info("Database and table initialized successfully.")

    except Error as e:
        logging.error(f"Error during database initialization: {e}")

    finally:
        cursor.close()
        conn.close()


def store_scheduler_metrics(
    timestamp: datetime, objectives_dict: dict, input_rate_dict: dict, framework: str
):
    logging.warning(
        "Scheduler_met: " + str(objectives_dict) + " " + str(input_rate_dict)
    )
    table_name = "scheduler_metrics"
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        my_cursor = conn.cursor()

        insert_query = f"""
        INSERT INTO {table_name} (timestamp, latency, cpu_load, throughput, input_rate_records_per_second,framework)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = (
            timestamp,
            objectives_dict["latency"],
            objectives_dict["cpu_load"],
            objectives_dict["throughput"],
            input_rate_dict["input_rate_records_per_second"],
            framework,
        )

        my_cursor.execute(insert_query, data)
        conn.commit()
    except Error as e:
        logging.error(f"Error inserting data: {e}")

    finally:
        my_cursor.close()
        conn.close()


def store_decision_in_db(
    timestamp: datetime, decision_dict: dict, going_to_switch=False
):
    logging.warning("Decision: " + str(decision_dict))
    start_times_table_name = "framework_start_times"
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO {start_times_table_name} (timestamp, framework_before, used_framework, u_sf, u_sl, going_to_switch)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = (
            timestamp,
            decision_dict["framework_before"],
            decision_dict["used_framework"],
            decision_dict["u_sf"],
            decision_dict["u_sl"],
            going_to_switch,
        )

        cursor.execute(insert_query, data)
        conn.commit()
        logging.info("Framework start time inserted successfully.")

    except Error as e:
        logging.error(f"Error inserting framework start time: {e}")

    finally:
        cursor.close()
        conn.close()


def store_historic_data(timestamp, metrics_dict: dict):
    table_name = "historic_metrics"
    logging.warning("Historic: " + str(metrics_dict))
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO {table_name} 
        (timestamp, latency, cpu_load, throughput, input_rate_records_per_second, framework) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        data = (
            timestamp,
            metrics_dict.get("latency"),
            metrics_dict.get("cpu_load"),
            metrics_dict.get("throughput"),
            metrics_dict.get("input_rate_records_per_second"),
            metrics_dict.get("framework"),
        )

        cursor.execute(insert_query, data)
        conn.commit()
        logging.info("Data inserted into historic_metrics table successfully.")
    except Error as e:
        logging.error(f"Error inserting framework start time: {e}")

    finally:
        cursor.close()
        conn.close()


def store_model_in_database(model_name, model_binary):
    table_name = "model_storage"

    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()
        cursor.execute(
            f"REPLACE INTO {table_name} (model_name, model_data) VALUES (%s, %s)",
            (model_name, model_binary),
        )
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Model saved to MySQL successfully.")
    except Exception as e:
        logging.error("Error when saving binary to database")
    finally:
        cursor.close()
        conn.close()


def store_processed_records_in_database(date, records, framework: str):
    table_name = "processed_metrics"

    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO {table_name} (timestamp, records, framework)
        VALUES (%s, %s, %s)
        """
        data = (date, records, framework)
        cursor.execute(insert_query, data)

        conn.commit()
        logging.info(f"Record inserted into '{table_name}' successfully.")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error when processed records to database")
    finally:
        cursor.close()
        conn.close()


def retrieve_single_model(model_name):
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()
        fetch_model_query = (
            f"SELECT model_data FROM model_storage WHERE model_name = %s"
        )
        cursor.execute(fetch_model_query, (model_name,))
        result = cursor.fetchone()
        if result:
            logging.info(f"Model '{model_name}' fetched successfully.")
            return result[0]
        else:
            logging.error(f"Model '{model_name}' not found.")
            return None
    except Exception as e:
        logging.error("Error when fetching load predictor model from database")
    finally:
        cursor.close()
        conn.close()


def retrieve_historic_data(framework: str):
    """
    Retrieve historic data for a specific framework.

    Args:
        framework (str): The framework for which historic data is to be retrieved.

    Returns:
        list[dict] | None: A list of dictionaries containing historic data for the specified framework,
        or None if an error occurs.
        Example:
            [
                {
                    'id': 18,
                    'timestamp': datetime.datetime(2024, 11, 4, 19, 44, 17),
                    'latency': 85.4,
                    'cpu_load': 0.25,
                    'throughput': 320.0,
                    'input_rate_records_per_second': 180.5,
                    'framework': 'SL'
                }
            ]
    """
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM historic_metrics WHERE framework = %s"
        cursor.execute(query, (framework,))
        rows = cursor.fetchall()
        return rows

    except mysql.connector.Error as err:
        logging.error(f"Error fetching historic data: {err}")
        return None


def retrieve_input_rates_current_data(since_timestamp=None):
    """
    Retrieve input rate records per second since a given timestamp.

    Args:
        since_timestamp (datetime | None): The starting timestamp to filter the records.
        If None, retrieves all records.

    Returns:
        list[dict]: A list of dictionaries containing `input_rate_records_per_second` and `timestamp`.
        Example:
            [{'input_rate_records_per_second': 500.0, 'timestamp': datetime.datetime(2024, 11, 4, 19, 44, 17)}, ...]
        If an error occurs, returns an empty list.
    """
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor(dictionary=True)
        if since_timestamp:
            query = """
            SELECT input_rate_records_per_second, timestamp 
            FROM scheduler_metrics
            WHERE timestamp >= %s 
            AND input_rate_records_per_second IS NOT NULL
            ORDER BY timestamp ASC
            """
            cursor.execute(query, (since_timestamp,))

        else:
            query = """
            SELECT input_rate_records_per_second, timestamp 
            FROM scheduler_metrics
            WHERE input_rate_records_per_second IS NOT NULL
            ORDER BY timestamp ASC
            """
            cursor.execute(query)

        result = cursor.fetchall()
        return result
    except mysql.connector.Error as err:
        logging.error(f"Error fetching data: {err}")
        return []


def retrieve_decisions():
    """
    Retrieve decisions from the `framework_start_times` table.

    Returns:
        list[dict] | None: A list of dictionaries containing decision records,
        or None if an error occurs.
        Example:
            [
                {
                    'id': 1,
                    'timestamp': datetime.datetime(2024, 11, 4, 19, 29, 55),
                    'used_framework': 'SL',
                    'u_sf': 0.9,
                    'u_sl': 0.2
                },
            ]
    """
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM  framework_start_times"
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

    except mysql.connector.Error as err:
        logging.error(f"Error fetching data: {err}")
        return None


def delete_tables():
    """
    Delete scheduler_metrics, framework_start_times, processed_metrics tables
    """
    logging.warning("Selected tables will be dropped")
    try:
        conn = mysql.connector.connect(**db_config, database=db_name)
        cursor = conn.cursor()

        tables = ["scheduler_metrics", "framework_start_times", "processed_metrics"]

        for table_name in tables:
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            cursor.execute(drop_query)
            logging.warning(f"Table '{table_name}' has been deleted.")

        conn.commit()
        logging.info("Selected tables deleted successfully.")

    except mysql.connector.Error as err:
        logging.error(f"Error deleting tables: {err}")
    finally:
        cursor.close()
        conn.close()
