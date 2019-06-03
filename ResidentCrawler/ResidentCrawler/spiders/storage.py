import configparser
import mysql.connector
import os
from mysql.connector import errorcode

config = configparser.ConfigParser()

dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, '../../config.ini')
config.read(filename)


def connect():
    try:
        return mysql.connector.connect(user=config['Database']['user'],
                                       password=config['Database']['password'],
                                       host=config['Database']['host'],
                                       database=config['Database']['database'])
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
