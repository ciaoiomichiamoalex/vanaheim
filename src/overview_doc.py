from calendar import Calendar
from datetime import date
from openpyxl.styles import Font
from share import sqlmng
from share.common import logger_ini
import openpyxl

PATH_PRJ = 'c:/source/vanaheim'
PATH_LOG = f"{PATH_PRJ}/log/vanaheim_{date.today().strftime('%Y_%m_%d')}.log"
PATH_RES = f'{PATH_PRJ}/res'
PATH_SCHEME = f'{PATH_PRJ}/scheme/consegne.xlsx'

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


def overview_gnr(anno: int = date.today().year, mese: int = date.today().month) -> None:
    logger = logger_ini(PATH_LOG, 'overview_doc')
    cursor = sqlmng.conx_ini()

    consegne = sqlmng.conx_read(cursor, QUERY_OVERVIEW_DATA, (anno, mese))
    if not consegne.rowcount:
        logger.warning(f'no record founded in {anno}/{mese:0>2}... skipping overview!')
        return

    wb = openpyxl.load_workbook(PATH_SCHEME)
    ws = wb['consegne']
    for row_num, row in enumerate(consegne.fetchall(), start=2):
        ws.cell(row=row_num, column=1).value = row.numero_documento
        ws.cell(row=row_num, column=1).font = Font(name='Arial')
        ws.cell(row=row_num, column=1).number_format = '0'

        ws.cell(row=row_num, column=2).value = row.data_documento
        ws.cell(row=row_num, column=2).font = Font(name='Arial')
        ws.cell(row=row_num, column=2).number_format = 'dd/mm'

        ws.cell(row=row_num, column=3).value = row.ragione_sociale
        ws.cell(row=row_num, column=3).font = Font(name='Arial')
        ws.cell(row=row_num, column=3).number_format = '@'

        ws.cell(row=row_num, column=4).value = row.sede_consegna
        ws.cell(row=row_num, column=4).font = Font(name='Arial')
        ws.cell(row=row_num, column=4).number_format = ' @'

        ws.cell(row=row_num, column=5).value = row.quantita
        ws.cell(row=row_num, column=5).font = Font(name='Arial')
        ws.cell(row=row_num, column=5).number_format = '#,##0'

        ws.cell(row=row_num, column=6).value = row.data_consegna
        ws.cell(row=row_num, column=6).font = Font(name='Arial')
        ws.cell(row=row_num, column=6).number_format = 'dd/mm'

        ws.cell(row=row_num, column=7).value = row.targa
        ws.cell(row=row_num, column=7).font = Font(name='Arial')
        ws.cell(row=row_num, column=7).number_format = '@'

    wsc = wb['cifre']
    wsl = wb['litri']
    for row_num, day in enumerate([date for date in Calendar().itermonthdates(anno, mese) if date.month == mese], start=3):
        wsc.cell(row=row_num, column=1).value = day
        wsc.cell(row=row_num, column=1).font = Font(name='Arial')
        wsc.cell(row=row_num, column=1).number_format = 'dd/mm'

        wsl.cell(row=row_num, column=1).value = day
        wsl.cell(row=row_num, column=1).font = Font(name='Arial')
        wsl.cell(row=row_num, column=1).number_format = 'dd/mm'

    logger.info(f'saving overview for {anno}/{mese:0>2}...')
    wb.save(f'{PATH_RES}/{anno}_{mese:0>2}.xlsx')


if __name__ == '__main__':
    overview_gnr()
