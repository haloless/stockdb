
import sqlite3
import threading

_DBCONN = threading.local()

_DBNAME = 'test.db'

def get_db_conn():
    '''Get DB connection as thread local'''
    global _DBCONN
    if not hasattr(_DBCONN, 'conn'):
        _DBCONN.conn = sqlite3.connect(_DBNAME)
    return _DBCONN.conn
####


# def create_db_conn():
#     return sqlite3.connect(_DBNAME)


