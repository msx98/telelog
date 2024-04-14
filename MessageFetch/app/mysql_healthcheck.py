import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import connection as MySQLConnection
import os
import time

def check_message_count():
    MYSQL_HOST = os.environ["MYSQL_HOST"]
    MYSQL_PORT = os.environ["MYSQL_PORT"]
    MYSQL_USER = os.environ["MYSQL_USER"]
    MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
    MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]
    conn = mysql.connector.connect(host=MYSQL_HOST,
                                    database=MYSQL_DATABASE,
                                    user=MYSQL_USER,
                                    password=MYSQL_PASSWORD,
                                    port=MYSQL_PORT,)
    if not conn.is_connected():
        raise Exception("Could not connect")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM messages")
    n = cur.fetchone()[0]
    cur.close()
    return n

n_1 = check_message_count()
time.sleep(10)
n_2 = check_message_count()

if n_1 == n_2:
    exit(1)
else:
    exit(0)
