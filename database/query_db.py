import psycopg2
import pandas as pd

from typing import Optional
from psycopg2 import sql
from database_constants import (
    DB_PARAMS,
    TYPE_MAPPING
)

def query_postgres_db(query: str):

    connection = psycopg2.connect(**DB_PARAMS)
    cursor = connection.cursor()

    try:
        query = sql.SQL("SELECT * FROM your_table;")
        cursor.execute(query)
        results = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(results, columns=columns)

    except Exception as e:
        print(f"Exception During Query : '{e}'")

    finally:
        cursor.close()
        connection.close()

def insert_into_table(data_to_insert, table_name):

    connection = psycopg2.connect(**DB_PARAMS)
    cursor = connection.cursor()

    try:
        columns = ', '.join(data_to_insert.columns)
        placeholders = ', '.join(['%s'] * len(data_to_insert.columns))

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.SQL(table_name),
            sql.SQL(columns),
            sql.SQL(placeholders)
        )

        data = [tuple(row) for _, row in data_to_insert.iterrows()]
        cursor.executemany(query, data)

        connection.commit()

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()

    finally:
        cursor.close()
        connection.close()

def generate_ddl_from_dataframe(
    df: pd.DataFrame, 
    table_name: str, 
    primary_key: Optional[str] = None) -> str:

    ddl = f"CREATE TABLE {table_name} (\n"

    for column_name, dtype in df.dtypes.items():
        sql_type = TYPE_MAPPING.get(dtype.name.lower(), dtype.name.upper())
        ddl += f"  {column_name} {sql_type},\n"

    if primary_key:
        ddl += f"  PRIMARY KEY ({primary_key})\n"

    ddl = ddl.rstrip(',\n')
    ddl += "\n);"

    return ddl

data_to_create_table = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 22],
})

table_name = 'example_table'
primary_key_column = 'id'

ddl_string = generate_ddl_from_dataframe(data_to_create_table, table_name, primary_key_column)
print(ddl_string)
