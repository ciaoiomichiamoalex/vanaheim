from share.common import decode_json
import pyodbc

PATH_PRJ = 'c:/source/vanaheim'
PATH_CFG = f'{PATH_PRJ}/config/sqlmng.json'


def conx_ini(conn_name: str = 'main', save_changes: bool = False) -> pyodbc.Cursor:
    config = decode_json(PATH_CFG, 'name', conn_name)
    if not config:
        raise ValueError(f'conx_ini: no config {conn_name} founded!')

    conx = pyodbc.connect(
        driver=f"{{{config[0]['driver']}}}",
        server=config[0]['server'],
        port=config[0]['port'],
        database=config[0]['database'],
        user=config[0]['user'],
        password=config[0]['password'],
        autocommit=save_changes
    )
    return conx.cursor()


def conx_read(cursor: pyodbc.Cursor, query: str, args: list | set | tuple = None) -> pyodbc.Cursor:
    return cursor.execute(*(query, args) if args else query)


def conx_write(cursor: pyodbc.Cursor, query: str, args: list | set | tuple = None) -> int:
    return cursor.execute(*(query, args) if args else query).rowcount
