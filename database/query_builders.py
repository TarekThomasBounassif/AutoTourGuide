import pandas as pd
from typing import Optional
from database.database_constants import (
    TYPE_MAPPING
)

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