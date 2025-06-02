import re
import os
import glob
import sqlite3
import warnings
import pandas as pd

DATASOURCE_PATH = "V:\\Escritorio\\DATA" #Actualizar método de obtención de la ruta

def extract_date(text):
    """
    Extracts the first substring matching YYYY-MM-DD from the given text.
    Returns the date string if found, else None.
    """
    match = re.search(r'\d{4}-\d{2}-\d{2}', text)
    return match.group(0) if match else None


def etl(database_connection, table_name, column_map, period=None, period_col="DATE", delimiter='\t'):
    """
    Default ETL process.
    Reads all files in a folder given file extension, asign sqlite datatypes acording to column_map,
    and uploads the result to the given table in the sqlite database.
    """
    columns_sql = [
        f"{new_name} {sqlite_type}"
        for _, (new_name, _, sqlite_type) in column_map.items()
    ]
    if period is not None:
        columns_sql.append("PERIODO DATE")
    columns_sql_str = ", ".join(columns_sql)

    # Initialize the table
    cur = database_connection.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql_str}
        )
    """)

    files = glob.glob(
        os.path.join(DATASOURCE_PATH + f"\\{table_name}", '*.csv')
        ) + glob.glob(
        os.path.join(DATASOURCE_PATH + f"\\{table_name}", '*.txt')
        )
    
    dataframes = []

    for f in files:
        try:
            df_temp = pd.read_csv(f, delimiter=delimiter, encoding='utf-8')
        except UnicodeDecodeError:
            print(f"Warning: File '{f}' is not UTF-8 encoded. Skipping.")
            continue
        df_temp = df_temp.drop(columns=[col for col in df_temp.columns if col not in column_map.keys()])
        if df_temp.empty:
            continue  # Skip this file if DataFrame is empty
        df_temp = df_temp.rename(columns={col: new_name for col, (new_name, _, _) in column_map.items()})
        df_temp = df_temp[sorted(df_temp.columns)]
        df_temp = df_temp.astype({col: dtype for col, (_, dtype, _) in column_map.items() if col in df_temp.columns})
        if period == 'filename':
            df_temp[period_col] = extract_date(os.path.basename(f))
        dataframes.append(df_temp)

    df = pd.concat(dataframes, ignore_index=True)
    periodos = df[period_col].unique().tolist() if period_col in df.columns else []

    if periodos:
        warnings.warn(f"Warning: The following {period_col} values will be replaced in the database: {periodos}")
        placeholders = ','.join('?' for _ in periodos)
        cur.execute(f"DELETE FROM {table_name} WHERE {period_col} IN ({placeholders})", periodos)

    df.to_sql(table_name, database_connection, if_exists='append', index=False)

    database_connection.commit()

    print("ETL finished successfully.")