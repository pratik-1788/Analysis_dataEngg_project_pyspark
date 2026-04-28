import mysql.connector

def get_mysql_connection():
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Pratik",
        database="spark"
    )
    return mysql_connection