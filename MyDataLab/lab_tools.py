import re


def extract_date(text):
    """
    Extracts the first substring matching YYYY-MM-DD from the given text.
    Returns the date string if found, else None.
    """
    match = re.search(r'\d{4}-\d{2}-\d{2}', text)
    return match.group(0) if match else None


def etl(database_connection, table_name, column_map, file_extension='csv'):
    """
    Default ETL process.
    Reads all files in a folder given file extension, asign sqlite datatypes acording to column_map,
    and uploads the result to the given table in the sqlite database.
    """
    pass
    