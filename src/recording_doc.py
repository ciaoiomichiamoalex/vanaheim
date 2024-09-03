from datetime import date
from share import sqlmng
from share.common import logger_ini
import os
import pypdfium2
import re

PATH_PRJ = 'c:/source/vanaheim'
PATH_LOG = f"{PATH_PRJ}/log/vanaheim_{date.strftime('%Y_%m_%d')}.log"
PATH_WORKING_DIR = f'{PATH_PRJ}/DDTs'
PATH_DISCARDED = f'{PATH_WORKING_DIR}/discarded'
PATH_RECORDED = f'{PATH_WORKING_DIR}/recorded'

PATTERN_WORKING_DOC = r'^\d{4}_\d{2}_DDT_\d{4}_\d{4}\.pdf$'
PATTERN_NUMERO_DATA = r'Num\. D\.D\.T\. ([\d\.]+)\/(\w{2}) Data D\.D\.T\. (\d{2}\/\d{2}\/\d{4}) Pag'
PATTERN_SEDE_SX = r"Luogo di partenza: .+\r\n([\w\s\.\&\-']+)\r\n(\d{5}) ([\w\s']+) \(?(\w{2})\)?\r\n"
PATTERN_SEDE_DX = r"Luogo di consegna\r\n([\w\s\.\&\-']+)\r\n.+\r\n(\d{0,5}) ?([\w\s']+) \(?(\w{2})\)?\r\n"
PATTERN_QUANTITA = r'(Quantità Prezzo\r\n.+)? (L|KG) ([\d\.]+),000\r\n'
PATTERN_TARGA = r'Peso soggetto accisa\r\n([\w\d]{7})\r\n'

QUERY_CHK_DUPLICATE = """
    SELECT COUNT(*)
    FROM consegne
    WHERE (
        sorgente = ?
        AND pagina = ?
    ) OR (
        numero_documento = ?
        AND genere_documento = ?
        AND YEAR(data_documento) = YEAR(?)
    );
"""
QUERY_INSERT = """
    INSERT INTO consegne (
        numero_documento,
        genere_documento,
        data_documento,
        ragione_sociale,
        sede_consegna,
        quantita,
        data_consegna,
        targa,
        sorgente,
        pagina
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def doc_scanner(working_doc: str) -> tuple[int, int]:
    doc = pypdfium2.PdfDocument(working_doc)
    discarded_pages = 0

    for working_page, page in enumerate(doc, start=1):
        logger.info(f'scanning on page {working_page} of {working_doc}...')
        text = page.get_textpage().get_text_bounded()

        doc_info = {}
        search = re.search(PATTERN_NUMERO_DATA, text)
        if search:
            doc_info['numero_documento'] = int(search.group(1).replace('.', ''))
            doc_info['genere_documento'] = search.group(1)
            doc_info['data_documento'] = search.group(2)[6:] + '-' + search.group(2)[3:5] + '-' + search.group(2)[0:2]
        else:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f'discarding page {working_page} of {working_doc} for error on PATTERN_NUMERO_DATA... [{discarded_doc}]')
            # TODO: save message on DB
            continue

        search = re.search(PATTERN_SEDE_DX, text)
        # TODO: check sede_consegna in DB comuni
        if search:
            doc_info['ragione_sociale'] = search.group(1)
            doc_info['sede_consegna'] = search.group(3)
        else:
            search = re.search(PATTERN_SEDE_SX, text)
            if search:
                doc_info['ragione_sociale'] = search.group(1)
                doc_info['sede_consegna'] = search.group(3)
            else:
                discarded_pages += 1
                discarded_doc = discard_doc(working_doc, working_page)
                logger.warning(f'discarding page {working_page} of {working_doc} for error on PATTERN_SEDE... [{discarded_doc}]')
                # TODO: save message on DB
                continue

        search = re.search(PATTERN_QUANTITA, text)
        if search:
            doc_info['quantita'] = int(search.group(3).replace('.', ''))
        else:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f'discarding page {working_page} of {working_doc} for error on PATTERN_QUANTITA... [{discarded_doc}]')
            # TODO: save message on DB
            continue

        doc_info['data_consegna'] = doc_info['data_documento']

        search = re.search(PATTERN_TARGA, text)
        if search:
            doc_info['targa'] = search.group(1)
        else:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f'discarding page {working_page} of {working_doc} for error on PATTERN_TARGA... [{discarded_doc}]')
            # TODO: save message on DB
            continue

        doc_info['sorgente'] = working_doc.replace('.recording', '')
        doc_info['pagina'] = working_page
        logger.debug(doc_info)

        cursor = sqlmng.conx_ini(save_changes=True)
        chk_dup = sqlmng.conx_read(cursor, QUERY_CHK_DUPLICATE, (
            doc_info['sorgente'],
            doc_info['pagina'],
            doc_info['numero_documento'],
            doc_info['genere_documento'],
            doc_info['data_documento']
        )).fetchone()[0]

        if chk_dup != 0:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.warning(f'discarding page {working_page} of {working_doc} because already recorded... [{discarded_doc}]')
            # TODO: save message on DB
            continue
        elif sqlmng.conx_write(cursor, QUERY_INSERT, doc_info) != 1:
            discarded_pages += 1
            discarded_doc = discard_doc(working_doc, working_page)
            logger.error(f'discarding page {working_page} of {working_doc} for error on saving record... [{discarded_doc}]')
            # TODO: save message on DB
            continue

    return len(doc), discarded_pages


def discard_doc(working_doc: str, working_page: int) -> str:
    doc = pypdfium2.PdfDocument(working_doc)
    discard = pypdfium2.PdfDocument().new()

    discard.import_pages(doc, [working_page - 1])
    discard_doc_name = f"{working_doc.split('.')[0]}_P{working_page:0>3}.pdf"
    discard.save(f'{PATH_DISCARDED}/{discard_doc_name}')
    return discard_doc_name


if __name__ == '__main__':
    logger = logger_ini(PATH_LOG)
    docs = next(os.walk(PATH_WORKING_DIR), (None, None, []))[2]

    logger.info(f'DDTs dir content {docs}')
    for doc in docs:
        if not re.search(PATTERN_WORKING_DOC, doc):
            logger.info(f'error on PATTERN_WORKING_DOC for {doc}... skipping doc!')
            continue

        working_doc = doc + '.recording'
        logger.info(f'working on doc {doc}...')
        os.rename(f'{PATH_WORKING_DIR}/{doc}', f'{PATH_WORKING_DIR}/{working_doc}')

        worked_pages, discarded_pages = doc_scanner(working_doc)

        logger.info(f'worked {worked_pages} pages on {doc} [{discarded_pages} discarded pages]')
        os.rename(f'{PATH_WORKING_DIR}/{working_doc}', f'{PATH_RECORDED}/{doc}.recorded')
