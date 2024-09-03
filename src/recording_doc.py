from datetime import date, datetime
from difflib import SequenceMatcher
from pyodbc import Cursor
from share import sqlmng
from share.common import logger_ini
from src.overview_doc import overview_gnr
import os
import pypdfium2
import re

PATH_PRJ = 'c:/source/vanaheim'
PATH_LOG = f"{PATH_PRJ}/log/vanaheim_{date.today().strftime('%Y_%m_%d')}.log"
PATH_WORKING_DIR = f'{PATH_PRJ}/DDTs'
PATH_DISCARDED = f'{PATH_WORKING_DIR}/discarded'
PATH_RECORDED = f'{PATH_WORKING_DIR}/recorded'

PATTERN_WORKING_DOC = r'^\d{4}_\d{2}_DDT_\d{4}_\d{4}(_P\d{3})*\.pdf$'
PATTERN_NUMERO_DATA = r'Num\. D\.D\.T\. ([\d\.]+)\/(\w{2}) Data D\.D\.T\. (\d{2}\/\d{2}\/\d{4}) Pag'
PATTERN_SEDE_SX = r"Luogo di partenza: .+\r\n([\w\s\.\&\-']+)\r\n(\d{5}) ([\w\s']+) \(?(\w{2})\)?\r\n"
PATTERN_SEDE_DX = r"Luogo di consegna\r\n([\w\s\.\&\-']+)\r\n.+\r\n(\d{0,5}) ?([\w\s']+) \(?(\w{2})\)?\r\n"
PATTERN_QUANTITA = r'(Quantità Prezzo\r\n.+)? (L|KG) ([\d\.]+),000\s'
PATTERN_TARGA = r'Peso soggetto accisa\r\n([\w\d]{7})\r\n'

QUERY_CHK_DUPLICATE = """
    SELECT COUNT(*)
    FROM vanaheim.consegne
    WHERE (
        sorgente = ?
        AND pagina = ?
    ) OR (
        numero_documento = ?
        AND genere_documento = ?
        AND EXTRACT(YEAR FROM data_documento) = ?
    );
"""
QUERY_INSERT_CONSEGNE = """
    INSERT INTO vanaheim.consegne (
        numero_documento,
        genere_documento,
        data_documento,
        ragione_sociale,
        sede_consegna,
        quantita,
        data_consegna,
        targa,
        sorgente,
        pagina,
        data_registrazione
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
QUERY_INSERT_MESSAGGI = """
    INSERT INTO vanaheim.messaggi (
        genere,
        testo
    ) VALUES (?, ?);
"""
QUERY_CHK_GAPS = """
    SELECT cg.numero, 
        cg.anno 
    FROM vanaheim.consegne_gap_vw cg 
        LEFT JOIN vanaheim.messaggi_gap_vw mg 
            ON cg.numero = mg.numero_documento 
            AND cg.anno = mg.anno 
    WHERE discarded IS FALSE
        AND mg.numero_documento IS NULL
    ORDER BY cg.anno, cg.numero;
"""
QUERY_CHK_RECORD_GAP = """
    SELECT id
    FROM vanaheim.messaggi_gap_vw
    WHERE numero_documento = ?
        AND anno = ?;
"""
QUERY_UPDATE_MESSAGGI = """
    UPDATE vanaheim.messaggi
    SET stato = FALSE
    WHERE id = ?;
"""
QUERY_OVERVIEW_DATE = """
    SELECT DISTINCT EXTRACT(YEAR FROM data_documento)::INT anno, EXTRACT(MONTH FROM data_documento)::INT mese
    FROM vanaheim.consegne 
    WHERE data_registrazione::DATE = ?::DATE
    ORDER BY 1, 2;
"""

ENUM_TARGA = [
    'ES745WH',
    'FC065ZW'
]

PATTERN_MESSAGE_DISCARD = {
    'genere': 'DISCARD',
    'testo': 'Page {page} of doc {doc} discarded for error on {pattern} [numero: {numero_documento}, genere: {genere_documento}, data: {data_documento}]'
}
PATTERN_MESSAGE_GAPS = {
    'genere': 'GAP',
    'testo': 'Finded gap for doc number {numero_documento} of year {anno}'
}


def doc_scanner(working_doc: str, cursor: Cursor, job_start: datetime = datetime.now()) -> tuple[int, int]:
    logger = logger_ini(PATH_LOG, 'recording_doc')
    doc = pypdfium2.PdfDocument(working_doc)
    page_numbers = len(doc)
    discarded_pages = 0

    for working_page, page in enumerate(doc, start=1):
        logger.info(f'scanning on page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]}...')
        text = page.get_textpage().get_text_bounded()

        doc_info = {}
        search = re.search(PATTERN_NUMERO_DATA, text)
        if search:
            doc_info['numero_documento'] = int(search.group(1).replace('.', ''))
            doc_info['genere_documento'] = search.group(2).upper()
            doc_info['data_documento'] = datetime.strptime(search.group(3)[6:] + '-' + search.group(3)[3:5] + '-' + search.group(3)[0:2], '%Y-%m-%d').date()
        else:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f"discarding page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]} for error on PATTERN_NUMERO_DATA... [{discarded_doc.split('/')[-1]}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, [PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'].format(
                page=working_page,
                doc=working_doc.replace('.recording', '').split('/')[-1],
                pattern='PATTERN_NUMERO_DATA',
                numero_documento=None,
                genere_documento=None,
                data_documento=None
            )]) != 1:
                logger.error('error on saving discard message record...')
            continue

        search = re.search(PATTERN_SEDE_DX, text)
        if search:
            doc_info['ragione_sociale'] = search.group(1).upper()
            doc_info['sede_consegna'] = search.group(3).upper()
        else:
            search = re.search(PATTERN_SEDE_SX, text)
            if search:
                doc_info['ragione_sociale'] = search.group(1).upper()
                doc_info['sede_consegna'] = search.group(3).upper()
            else:
                discarded_pages += 1
                discarded_doc = discard_doc(working_doc, working_page)
                logger.warning(f"discarding page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]} for error on PATTERN_SEDE... [{discarded_doc.split('/')[-1]}]")
                if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, [PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'].format(
                    page=working_page,
                    doc=working_doc.replace('.recording', '').split('/')[-1],
                    pattern='PATTERN_SEDE',
                    numero_documento=doc_info['numero_documento'],
                    genere_documento=doc_info['genere_documento'],
                    data_documento=doc_info['data_documento']
                )]) != 1:
                    logger.error('error on saving discard message record...')
                continue
        # TODO: check sede_consegna in DB comuni

        search = re.search(PATTERN_QUANTITA, text)
        if search:
            doc_info['quantita'] = int(search.group(3).replace('.', ''))
        else:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f"discarding page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]} for error on PATTERN_QUANTITA... [{discarded_doc.split('/')[-1]}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, [PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'].format(
                page=working_page,
                doc=working_doc.replace('.recording', '').split('/')[-1],
                pattern='PATTERN_QUANTITA',
                numero_documento=doc_info['numero_documento'],
                genere_documento=doc_info['genere_documento'],
                data_documento=doc_info['data_documento']
            )]) != 1:
                logger.error('error on saving discard message record...')
            continue

        doc_info['data_consegna'] = doc_info['data_documento']

        search = re.search(PATTERN_TARGA, text)
        if search:
            targa = search.group(1).upper()
            doc_info['targa'] = targa if targa in ENUM_TARGA else check_targa(targa)
        else:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f"discarding page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]} for error on PATTERN_TARGA... [{discarded_doc.split('/')[-1]}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, [PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'].format(
                page=working_page,
                doc=working_doc.replace('.recording', '').split('/')[-1],
                pattern='PATTERN_TARGA',
                numero_documento=doc_info['numero_documento'],
                genere_documento=doc_info['genere_documento'],
                data_documento=doc_info['data_documento']
            )]) != 1:
                logger.error('error on saving discard message record...')
            continue

        doc_info['sorgente'] = working_doc.replace('.recording', '').split('/')[-1]
        doc_info['pagina'] = working_page
        doc_info['data_registrazione'] = job_start
        logger.debug(doc_info)

        chk_dup = sqlmng.conx_read(cursor, QUERY_CHK_DUPLICATE, (
            doc_info['sorgente'],
            doc_info['pagina'],
            doc_info['numero_documento'],
            doc_info['genere_documento'],
            doc_info['data_documento'].year
        )).fetchone()[0]

        if chk_dup:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f"discarding page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]} because already recorded... [{discarded_doc.split('/')[-1]}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, [PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'].format(
                page=working_page,
                doc=working_doc.replace('.recording', '').split('/')[-1],
                pattern='QUERY_CHK_DUPLICATE',
                numero_documento=doc_info['numero_documento'],
                genere_documento=doc_info['genere_documento'],
                data_documento=doc_info['data_documento']
            )]) != 1:
                logger.error('error on saving discard message record...')
            continue
        elif sqlmng.conx_write(cursor, QUERY_INSERT_CONSEGNE, [value for value in doc_info.values()]) != 1:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.error(f"discarding page {working_page} of {working_doc.replace('.recording', '').split('/')[-1]} for error on saving record... [{discarded_doc.split('/')[-1]}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, [PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'].format(
                page=working_page,
                doc=working_doc.replace('.recording', '').split('/')[-1],
                pattern='QUERY_INSERT_CONSEGNE',
                numero_documento=doc_info['numero_documento'],
                genere_documento=doc_info['genere_documento'],
                data_documento=doc_info['data_documento']
            )]) != 1:
                logger.error('error on saving discard message record...')

        chk_gap = sqlmng.conx_read(cursor, QUERY_CHK_RECORD_GAP, (
            doc_info['numero_documento'],
            doc_info['data_documento'].year
        )).fetchone()
        if chk_gap and sqlmng.conx_write(cursor, QUERY_UPDATE_MESSAGGI, chk_gap[0]) != 1:
            logger.error(f'error on update message status... [message id: {chk_gap}]')

    doc.close()
    return page_numbers, discarded_pages


def discard_doc(working_doc: str, working_page: int) -> str:
    doc = pypdfium2.PdfDocument(working_doc)
    discard = pypdfium2.PdfDocument.new()

    discard.import_pages(doc, [working_page - 1])
    discard_doc_name = f"{working_doc.split('.')[0]}_P{working_page:0>3}.pdf"
    discard.save(f"{PATH_DISCARDED}/{discard_doc_name.split('/')[-1]}")

    doc.close()
    return discard_doc_name


def check_targa(doc_targa: str) -> str:
    logger = logger_ini(PATH_LOG, 'recording_doc')
    logger.info(f'checking similarity for {doc_targa}...')

    enum = dict.fromkeys(ENUM_TARGA, 0.0)
    for targa in enum:
        enum[targa] = SequenceMatcher(None, targa, doc_targa).ratio()

    max_score = sorted(enum, key=enum.get, reverse=True)[0]
    return max_score if enum[max_score] > 0.5 else doc_targa


if __name__ == '__main__':
    logger = logger_ini(PATH_LOG, 'recording_doc')
    docs = sorted(next(os.walk(PATH_WORKING_DIR), (None, None, []))[2])

    logger.info(f'DDTs dir content {docs}')
    job_start = datetime.now()
    cursor = sqlmng.conx_ini(save_changes=True)
    for doc in docs:
        if not re.search(PATTERN_WORKING_DOC, doc):
            logger.info(f'error on PATTERN_WORKING_DOC for {doc}... skipping doc!')
            continue

        working_doc = doc + '.recording'
        logger.info(f'working on doc {doc}...')
        os.rename(f'{PATH_WORKING_DIR}/{doc}', f'{PATH_WORKING_DIR}/{working_doc}')

        worked_pages, discarded_pages = doc_scanner(f'{PATH_WORKING_DIR}/{working_doc}', cursor, job_start)

        logger.info(f'worked {worked_pages} pages on {doc} [{discarded_pages} discarded pages]')
        os.rename(f'{PATH_WORKING_DIR}/{working_doc}', f'{PATH_RECORDED}/{doc}.recorded')

    gaps = sqlmng.conx_read(cursor, QUERY_CHK_GAPS).fetchall()
    for row in gaps:
        if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI,[PATTERN_MESSAGE_GAPS['genere'], PATTERN_MESSAGE_GAPS['testo'].format(
            numero_documento=row.numero,
            anno=row.anno
        )]) != 1:
            logger.error('error on saving gap message record...')

    overviews = sqlmng.conx_read(cursor, QUERY_OVERVIEW_DATE, job_start).fetchall()
    for row in overviews:
        overview_gnr(row.anno, row.mese)

    cursor.close()
