from CreateTables import *
from LoadData import *
from UploadData import *
from Constants import *


def connectToDatabase(dbname, user, password, host, port):
    # Open a cursor to perform database operations
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port);
# Connect to an existing database




def dropAndCreateDatabase():
    try:
        conn = psycopg.connect(
                user=USER,
                password=PASSWORD,
                host=HOST,
                port=PORT
            )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
        cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
        cursor.close()
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

dropAndCreateDatabase()
deleteAndRecreateTables()
loadAll()
createTablesAndReferences()
dropAndCreateIndexes()