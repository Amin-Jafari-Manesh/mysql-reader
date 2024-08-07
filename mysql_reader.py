import logging
import time
from os import environ
import mysql.connector

logging.basicConfig(level=logging.INFO)

db_config = {
    'PASS': environ.get('PASS', ''),
    'DOMAIN': environ.get('DOMAIN', ''),
    'DATA_TYPE': environ.get('DATA_TYPE', ''),
    'INSERT_DELAY': int(environ.get('INSERT_DELAY', '')),
}

conn = mysql.connector.connect(
    host=db_config['DOMAIN'],
    user='admin',
    password=db_config['PASS'],
    database='db',
    port='3306'
)


def mysql_read() -> bool:
    if conn and conn.is_connected():
        logging.info("Connected to the database.")
        cur = conn.cursor()
        if db_config['DATA_TYPE'] == 'h':
            while True:
                time.sleep(db_config['INSERT_DELAY'] * 0.001)
                cur.execute(f'SELECT * FROM hashes ORDER BY "created_at" DESC LIMIT 100')
                conn.commit()
        elif db_config['DATA_TYPE'] == 't':
            while True:
                time.sleep(db_config['INSERT_DELAY'] * 0.001)
                cur.execute(f'SELECT * FROM text ORDER BY "created_at" DESC LIMIT 100')
                conn.commit()
        else:
            logging.error("Invalid data type.")
            return False
    else:
        logging.error("Failed to connect to the database.")
        return False
    return True


if __name__ == '__main__':
    if mysql_read():
        logging.info("Hashes successfully written to the database.")
    else:
        logging.error("Failed to write hashes to the database.")
