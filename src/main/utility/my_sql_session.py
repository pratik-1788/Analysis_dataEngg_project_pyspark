import mysql.connector

def get_mysql_connection():
    mysql_connection = mysql.connector.connect(
        host=" 172.19.176.1",
        user="root",
        password="Pratik",
        database="spark"
    )
    return mysql_connection