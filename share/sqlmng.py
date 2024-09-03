from share.common import decode_json
import pyodbc

PRJ_PATH = 'c:/source/vanaheim'
CFG_PATH = f'{PRJ_PATH}/config/sqlmng.json'


def conx_ini(conn_name: str = 'main', save_changes: bool = False) -> pyodbc.Cursor:
    config = decode_json(CFG_PATH, 'name', conn_name)
    if not config:
        raise ValueError(f'conx_ini: no config {conn_name} founded!')

    conx = pyodbc.connect(
        driver=f"{{{config['driver']}}}",
        server=config['server'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password'],
        autocommit=save_changes
    )
    return conx.cursor()


def conx_read(cursor: pyodbc.Cursor, query: str, args: dict | list | set | tuple = None) -> pyodbc.Cursor:
    return cursor.execute(*(query, args) if args else query)


def conx_write(cursor: pyodbc.Cursor, query: str, args: dict | list | set | tuple = None) -> int:
    return cursor.execute(*(query, args) if args else query).rowcount
