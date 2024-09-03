from share.common import decode_json
import pyodbc

PATH_PRJ = 'c:/source/vanaheim'
PATH_CFG = f'{PATH_PRJ}/config/sqlmng.json'


def conx_ini(conn_name: str = 'main', save_changes: bool = False) -> tuple[pyodbc.Cursor, pyodbc.Connection]:
    """Read from config/sqlmng.json the database configuration and start the connection.

    :param str conn_name: Refers to the database configuration name in sqlmng.json, defaults to 'main'.
    :param bool save_changes: Enables or disables the auto-commit, defaults to False.
    :return: A tuple with the cursor and the reference to connection.
    """
    config = decode_json(PATH_CFG, 'name', conn_name)
    if not config:
        raise ValueError(f'conx_ini: no config {conn_name} found!')

    conx = pyodbc.connect(
        driver=f"{{{config[0]['driver']}}}",
        server=config[0]['server'],
        port=config[0]['port'],
        database=config[0]['database'],
        user=config[0]['user'],
        password=config[0]['password'],
        autocommit=save_changes
    )
    return conx.cursor(), conx


def conx_read(cursor: pyodbc.Cursor, query: str, args: list | set | tuple = None) -> pyodbc.Cursor:
    """Execute a DQL query on the database (SELECT).

    :param Cursor cursor: The cursor achieved from conx_ini() calling.
    :param str query: Query string to be executed.
    :param list or set or tuple args: The parameters list of the query string, defaults to None.
    :return: The cursor with values.
    """
    return cursor.execute(query, args) if args else cursor.execute(query)


def conx_write(cursor: pyodbc.Cursor, query: str, args: list | set | tuple = None) -> int:
    """Execute a DDL or DML query on the database (INSERT, UPDATE, DELETE, CREATE, ...).

    :param Cursor cursor: The cursor achieved from conx_ini() calling.
    :param str query: Query string to be executed.
    :param list or set or tuple args: The parameters list of the query string, defaults to None.
    :return: The number of affected rows.
    """
    return cursor.execute(query, args).rowcount if args else cursor.execute(query).rowcount


def column_names(cursor: pyodbc.Cursor) -> list[str] | None:
    """Get the list of column names.

    :param Cursor cursor: The cursor to the database.
    :return: A list of column names or None if there isn't columns.
    """
    return [column[0] for column in cursor.description]
