from datetime import date
from share.common import decode_json, logger_ini
import pyodbc

PRJ_PATH = 'c:/source/vanaheim'
CFG_PATH = f'{PRJ_PATH}/config/sqlmng.json'
LOG_PATH = f'{PRJ_PATH}/log/vanaheim_{date.strftime('%Y_%m_%d')}.log'


def conx_ini(conn_name: str = 'main', save_changes: bool = False) -> pyodbc.Cursor:
    config = decode_json(CFG_PATH, 'name', conn_name)
    logger = logger_ini(LOG_PATH)
    if not config:
        logger.error(f'conx_ini: no config {conn_name} founded!')
        quit(-1)

    try:
        conx = pyodbc.connect(
            driver=f"{{{config['driver']}}}",
            server=config['server'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            autocommit=save_changes
        )

        logger.info(f'conx_ini: starting connection on config {conn_name}...')
        return conx.cursor()
    except pyodbc.Error as error:
        logger.exception(f'connection error on config {conn_name}!')
        quit(-2)


def conx_read(cursor: pyodbc.Cursor, query: str, args: dict | list | set | tuple = None) -> pyodbc.Cursor:
    return cursor.execute(*(query, args) if args else query)


def conx_write(cursor: pyodbc.Cursor, query: str, args: dict | list | set | tuple = None) -> int:
    return cursor.execute(*(query, args) if args else query).rowcount
