import logging
import time
from os import environ
import mysql.connector
from mysql.connector import Error

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE': environ.get('DATA_TYPE', ''),
    'INSERT_DELAY': int(environ.get('INSERT_DELAY', 0)),
}


def connect_to_database():
    try:
        conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )
        if conn.is_connected():
            logging.info("Connected to the database.")
            return conn
    except Error as e:
        logging.error(f"Error connecting to the database: {e}")
    return None


def mysql_read(conn) -> bool:
    if not conn or not conn.is_connected():
        logging.error("Not connected to the database.")
        return False

    try:
        cur = conn.cursor()
        query = ''
        if db_config['DATA_TYPE'] == 'h':
            query = 'SELECT * FROM hashes ORDER BY created_at DESC LIMIT 100'
        elif db_config['DATA_TYPE'] == 't':
            query = 'SELECT * FROM text ORDER BY created_at DESC LIMIT 100'
        else:
            logging.error("Invalid data type.")
            return False

        while True:
            time.sleep(db_config['INSERT_DELAY'] * 0.001)
            cur.execute(query)
            results = cur.fetchall()
            logging.info(f"Retrieved {len(results)} rows.")
            # Process the results as needed
    except Error as e:
        logging.error(f"Database error: {e}")
        return False
    finally:
        if cur:
            cur.close()


def main():
    conn = connect_to_database()
    if conn:
        try:
            if mysql_read(conn):
                logging.info("Data successfully read from the database.")
            else:
                logging.error("Failed to read data from the database.")
        finally:
            conn.close()
            logging.info("Database connection closed.")
    else:
        logging.error("Failed to connect to the database.")


if __name__ == '__main__':
    main()
