import logging
from datetime import datetime
import time
from os import environ
import mysql.connector

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'INSERT_DELAY' : int(environ.get('INSERT_DELAY', '')),
}

def test_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )
        logging.info("Successfully connected to MySQL")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"MySQL connection failed: {e}")
        return False


def mysql_read() -> bool:
    if test_mysql_connection():
        conn = mysql.connector.connect(
            host=db_config['DOMAIN'],
            user='admin',
            password=db_config['PASS'],
            database='db',
            port='3306'
        )
        cur = conn.cursor()
        while True:
            time.sleep(db_config['INSERT_DELAY']*0.001)
            cur.execute(f'SELECT * FROM hashes ORDER BY "created_at" DESC LIMIT 100')
            conn.commit()
        conn.close()
        return True
    return False


if __name__ == '__main__':
    if mysql_read(db_config['RECORDS']):
        logging.info("Hashes successfully written to the database.")
    else:
        logging.error("Failed to write hashes to the database.")
