from calendar import Calendar
from datetime import date
from openpyxl.styles import Font
from share import sqlmng
from share.common import logger_ini
import openpyxl

PATH_PRJ = 'c:/source/vanaheim'
PATH_LOG = f"{PATH_PRJ}/log/vanaheim_{date.today().strftime('%Y_%m_%d')}.log"
PATH_RES = f'{PATH_PRJ}/res'
PATH_SCHEME = f'{PATH_PRJ}/scheme'

QUERY_OVERVIEW_DATA = """
    SELECT numero_documento,
        data_documento,
        ragione_sociale,
        sede_consegna,
        quantita,
        data_consegna,
        targa
    FROM vanaheim.consegne
    WHERE EXTRACT(YEAR FROM data_consegna) = ?
        AND EXTRACT(MONTH FROM data_consegna) = ?
    ORDER BY numero_documento;
"""
QUERY_SUMMARY_VIAGGI = """
    WITH viaggi AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY targa ORDER BY data_consegna, sede_consegna) AS id,
            targa,
            data_consegna, 
            sede_consegna
        FROM vanaheim.consegne
        WHERE EXTRACT(YEAR FROM data_documento) = 2024
    ) 
    SELECT v1.data_consegna data_es745wh,
        v1.sede_consegna sede_es745wh,
        v2.sede_consegna sede_fc065zw,
        v2.data_consegna data_fc065zw
    FROM (
        SELECT id,
            data_consegna,
            sede_consegna
        FROM viaggi
        WHERE targa = 'ES745WH'
    ) v1
        FULL JOIN (
            SELECT id, 
                data_consegna,
                sede_consegna
            FROM viaggi
            WHERE targa = 'FC065ZW'
        ) v2 ON v1.id = v2.id
    ORDER BY COALESCE(v1.id, v2.id);
"""

DEFAULT_FONT = 'Arial'
FORMATS = {
    str: '@',
    int: '#,##0',
    date: 'dd/mm',
    'number': '0'
}


def overview_gnr(anno: int = date.today().year, mese: int = date.today().month) -> None:
    """Generate the overview excel by taking data from the database.

    :param int anno: The desired year for the overview.
    :param int mese: The desired month for the overview.
    """
    logger = logger_ini(PATH_LOG, 'overview_doc')
    cursor = sqlmng.conx_ini()

    consegne = sqlmng.conx_read(cursor, QUERY_OVERVIEW_DATA, (anno, mese)).fetchall()
    if not consegne:
        logger.warning(f'no record founded in {anno}/{mese:0>2}... skipping overview!')
        return

    wb = openpyxl.load_workbook(f'{PATH_SCHEME}/consegne.xlsx')
    ws = wb['consegne']
    for row_num, row in enumerate(consegne, start=2):
        for col_num, col in enumerate(row, start=1):
            ws.cell(row=row_num, column=col_num).value = col
            ws.cell(row=row_num, column=col_num).font = Font(name=DEFAULT_FONT)
            ws.cell(row=row_num, column=col_num).number_format = FORMATS[type(col)] if col_num != 1 else FORMATS['number']

    wsc = wb['cifre']
    wsl = wb['litri']
    wsc.cell(row=1, column=1).value = date.today().year
    wsc.cell(row=1, column=1).font = Font(name=DEFAULT_FONT)
    wsc.cell(row=1, column=1).number_format = FORMATS['number']
    wsl.cell(row=1, column=1).value = date.today().year
    wsl.cell(row=1, column=1).font = Font(name=DEFAULT_FONT)
    wsl.cell(row=1, column=1).number_format = FORMATS['number']

    for row_num, day in enumerate([d for d in Calendar().itermonthdates(anno, mese) if d.month == mese], start=3):
        wsc.cell(row=row_num, column=1).value = day
        wsc.cell(row=row_num, column=1).font = Font(name=DEFAULT_FONT)
        wsc.cell(row=row_num, column=1).number_format = FORMATS[date]

        wsl.cell(row=row_num, column=1).value = day
        wsl.cell(row=row_num, column=1).font = Font(name=DEFAULT_FONT)
        wsl.cell(row=row_num, column=1).number_format = FORMATS[date]
    # TODO: remove extra days (31/06, 30/02, ...)

    logger.info('saving overview for {0}... [{0}.xlsx]'.format(f'{anno}_{mese:0>2}'))
    wb.save(f'{PATH_RES}/{anno}_{mese:0>2}.xlsx')
    cursor.close()


def summary_viaggi(anno: int = date.today().year) -> None:
    # TODO: save summary of travels (data_consegna, sede_consegna)
    pass


if __name__ == '__main__':
    # TODO: make mese optional in overview_gnr()
    overview_gnr()
