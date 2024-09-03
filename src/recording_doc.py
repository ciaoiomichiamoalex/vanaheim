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
PATH_DISCARDED_DIR = f'{PATH_WORKING_DIR}/discarded'
PATH_RECORDED_DIR = f'{PATH_WORKING_DIR}/recorded'

PATTERN_WORKING_DOC = r'^\d{4}_\d{2}_DDT_\d{4}_\d{4}(_P\d{3})*\.pdf$'
PATTERN_NUMERO_DATA = r'Num\. D\.D\.T\. ([\d\.]+)\/(\w{2}) Data D\.D\.T\. (\d{2}\/\d{2}\/\d{4}) Pag'
PATTERN_SEDE_SX = r"Luogo di partenza: .+\r\n([\w\s\.\&\-']+)\r\n(\d{5}) ([\w\s']+) \(?(\w{2})\)?\r\n"
PATTERN_SEDE_DX = r"Luogo di consegna\r\n([\w\s\.\&\-']+)\r\n.+\r\n(\d{0,5}) ?([\w\s']+) \(?(\w{2})\)?\r\n"
PATTERN_QUANTITA = r'(Quantità Prezzo\r\n.+)? (L|KG) ([\d\.]+),000\s'
PATTERN_TARGA = r'Peso soggetto accisa\r\n([\w\d]{7})\r\n'

QUERY_CHK_DUPLICATE = """
    SELECT COUNT(*) nr_record
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
    ) VALUES (?, ?)
    RETURNING id;
"""
QUERY_CHK_GAPS = """
    SELECT cg.numero, 
        cg.anno 
    FROM vanaheim.consegne_gap_vw cg 
        LEFT JOIN vanaheim.messaggi_gap_vw mg 
            ON cg.numero = mg.numero_documento 
            AND cg.anno = mg.anno 
    WHERE cg.discarded IS FALSE
        AND mg.numero_documento IS NULL
    ORDER BY cg.anno, cg.numero;
"""
QUERY_CHK_RECORD_GAP = """
    SELECT id
    FROM vanaheim.messaggi_gap_vw
    WHERE numero_documento = ?
        AND anno = ?
        AND stato IS TRUE;
"""
QUERY_UPDATE_MESSAGGI = """
    UPDATE vanaheim.messaggi
    SET stato = FALSE
    WHERE id = ?;
"""
QUERY_OVERVIEW_DATE = """
    SELECT DISTINCT EXTRACT(YEAR FROM data_documento) anno, 
        EXTRACT(MONTH FROM data_documento) mese
    FROM vanaheim.consegne 
    WHERE data_registrazione = ?
    ORDER BY 1, 2;
"""
QUERY_INSERT_DISCARD_CONSEGNE = """
    INSERT INTO vanaheim.discard_consegne (
        numero_documento, 
        genere_documento,
        data_documento,
        ragione_sociale, 
        sede_consegna,
        quantita,
        data_consegna,
        targa,
        sorgente,
        id_messaggio 
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
QUERY_CHK_DISCARD_CONSEGNE = """
    SELECT numero_documento,
        genere_documento,
        data_documento,
        ragione_sociale,
        sede_consegna,
        quantita,
        data_consegna,
        targa
        id_messaggio 
    FROM vanaheim.discard_consegne 
    WHERE stato IS TRUE 
        AND sorgente = ?;
"""

ENUM_TARGA = [
    'ES745WH',
    'FC065ZW'
]

PATTERN_MESSAGE_DISCARD = {
    'genere': 'DISCARD',
    'testo': 'Page %(page)d of doc %(doc)s discarded for error on %(pattern)s [numero: %(numero_documento)d, genere: %(genere_documento)s, data: %(data_documento)s]'
}
PATTERN_MESSAGE_GAPS = {
    'genere': 'GAP',
    'testo': 'Found gap for doc number %(numero_documento)d of year %(anno)d'
}
PATTERN_MESSAGE_SIMILARITY_CRASH = {
    'genere': 'WARNING',
    'testo': 'Had similarity crash for %(targa)s on page %(page)d of doc %(doc)s'
}


def doc_scanner(working_doc: str, cursor: Cursor, recording_begin: datetime = datetime.now()) -> tuple[int, int]:
    """Parse the information from pdf document and save on database.

    :param str working_doc: Path to the pdf document.
    :param Cursor cursor: The cursor to the database.
    :param datetime recording_begin: The starting timestamp of the process, defaults to now().
    :return: A tuple containing the number of document page and number of discarded page.
    """
    logger = logger_ini(PATH_LOG, 'recording_doc')
    # working_doc: percorso con suffisso '.recording' (es. c:/source/vanaheim/DDTs/2024_01_DDT_0001_0267.pdf.recording)
    # doc: raw PDF doc
    doc = pypdfium2.PdfDocument(working_doc)
    # doc_pages: numero totale di pagine di working_doc
    doc_pages = len(doc)
    # discarded_pages: dizionario con informazioni sulle pagine in errore
    discarded_pages = {
        # number: numero di pagine in errore
        'number': 0,
        # is_discarded: vero se un'estrazione su page è in errore
        'is_discarded': False,
        # discard_message: messaggio da salvare se is_discarded è vero
        'discard_message': None
    }

    # working_doc_name: basename del doc in registrazione working_doc (es. 2024_01_DDT_0001_0267.pdf)
    working_doc_name = working_doc.replace('.recording', '').split('/')[-1]

    # working_page: numero di pagina in registrazione
    # page: raw PDF page
    for working_page, page in enumerate(doc, start=1):
        logger.info(f'scanning on page {working_page} of {working_doc_name}...')
        text = page.get_textpage().get_text_bounded()

        # doc_info: dizionario con informazioni estratte da page
        doc_info = {}

        # estrazione numero_documento, genere_documento e data_documento
        search = re.search(PATTERN_NUMERO_DATA, text)
        doc_info['numero_documento'] = int(search.group(1).replace('.', '')) if search else None
        doc_info['genere_documento'] = search.group(2).upper() if search else None
        doc_info['data_documento'] = datetime.strptime(search.group(3)[6:] + '-' + search.group(3)[3:5] + '-' + search.group(3)[0:2], '%Y-%m-%d').date() if search else None

        if not search:
            logger.warning(f"discarding page {working_page} of {working_doc_name} for error on PATTERN_NUMERO_DATA...")
            if not discarded_pages['is_discarded']:
                discarded_pages['discard_message'] = (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                    'page': working_page,
                    'doc': working_doc_name,
                    'pattern': 'PATTERN_NUMERO_DATA',
                    'numero_documento': None,
                    'genere_documento': None,
                    'data_documento': None
                })
            discarded_pages['is_discarded'] = True

        # estrazione sede_consegna
        search = re.search(PATTERN_SEDE_DX, text)
        if search:
            doc_info['ragione_sociale'] = search.group(1).upper()
            doc_info['sede_consegna'] = search.group(3).upper()
        else:
            search = re.search(PATTERN_SEDE_SX, text)
            doc_info['ragione_sociale'] = search.group(1).upper() if search else None
            doc_info['sede_consegna'] = search.group(3).upper() if search else None

            if not search:
                logger.warning(f"discarding page {working_page} of {working_doc_name} for error on PATTERN_SEDE...")
                if not discarded_pages['is_discarded']:
                    discarded_pages['discard_message'] = (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                        'page': working_page,
                        'doc': working_doc_name,
                        'pattern': 'PATTERN_SEDE',
                        'numero_documento': doc_info['numero_documento'],
                        'genere_documento': doc_info['genere_documento'],
                        'data_documento': doc_info['data_documento']
                    })
                discarded_pages['is_discarded'] = True

        # estrazione quantità
        search = re.search(PATTERN_QUANTITA, text)
        doc_info['quantita'] = int(search.group(3).replace('.', '')) if search else None

        if not search:
            logger.warning(f"discarding page {working_page} of {working_doc_name} for error on PATTERN_QUANTITA...")
            if not discarded_pages['is_discarded']:
                discarded_pages['discard_message'] = (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                    'page': working_page,
                    'doc': working_doc_name,
                    'pattern': 'PATTERN_QUANTITA',
                    'numero_documento': doc_info['numero_documento'],
                    'genere_documento': doc_info['genere_documento'],
                    'data_documento': doc_info['data_documento']
                })
            discarded_pages['is_discarded'] = True

        # duplico data_documento in data_consegna
        doc_info['data_consegna'] = doc_info['data_documento']

        # estrazione targa
        search = re.search(PATTERN_TARGA, text)
        if search:
            targa = search.group(1).upper()
            doc_info['targa'] = targa if targa in ENUM_TARGA else check_similarity(targa)

            if doc_info['targa'] not in ENUM_TARGA and sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, (PATTERN_MESSAGE_SIMILARITY_CRASH['genere'], PATTERN_MESSAGE_SIMILARITY_CRASH['testo'] % {
                'targa': doc_info['targa'],
                'page': working_page,
                'doc': working_doc_name
            })) != 1:
                logger.error('error on saving similarity crash message record...')
        else:
            doc_info['targa'] = None

            logger.warning(f"discarding page {working_page} of {working_doc_name} for error on PATTERN_TARGA...")
            if not discarded_pages['is_discarded']:
                discarded_pages['discard_message'] = (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                    'page': working_page,
                    'doc': working_doc_name,
                    'pattern': 'PATTERN_TARGA',
                    'numero_documento': doc_info['numero_documento'],
                    'genere_documento': doc_info['genere_documento'],
                    'data_documento': doc_info['data_documento']
                })
            discarded_pages['is_discarded'] = True

        # controllo se page è in errore
        if discarded_pages['is_discarded']:
            is_many_discard = re.findall(r'_P\d{3}', working_doc_name)
            chk_discard = sqlmng.conx_read(cursor, QUERY_CHK_DISCARD_CONSEGNE, [re.sub(r'(_P\d{3}){2,}', is_many_discard[0], working_doc_name) if len(is_many_discard) > 1 else working_doc_name]).fetchone()

            # controllo se esiste record di scarto
            if chk_discard:
                if None not in chk_discard:
                    # controllo se è un duplicato
                    chk_dup = sqlmng.conx_read(cursor, QUERY_CHK_DUPLICATE, (
                        working_doc_name,
                        working_page,
                        chk_discard.numero_documento,
                        chk_discard.genere_documento,
                        chk_discard.data_documento.year
                    )).fetchone()[0]

                    if chk_dup:
                        discarded_pages['number'] += 1
                        discarded_doc = discard_doc(working_doc, working_page)
                        discarded_doc_name = discarded_doc.split('/')[-1]

                        logger.warning(f"discarding page {working_page} of {working_doc_name} because already recorded... [{discarded_doc_name}]")
                        if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                            'page': working_page,
                            'doc': working_doc_name,
                            'pattern': 'QUERY_CHK_DUPLICATE',
                            'numero_documento': doc_info['numero_documento'],
                            'genere_documento': doc_info['genere_documento'],
                            'data_documento': doc_info['data_documento']
                        })) != 1:
                            logger.error('error on saving discard message record...')
                        continue
                    elif sqlmng.conx_write(cursor, QUERY_INSERT_CONSEGNE, (
                        *chk_discard[:-1],
                        working_doc_name,
                        working_page,
                        recording_begin
                    )) != 1:
                        discarded_pages['number'] += 1
                        # discarded_doc: percorso del doc di scarto (es. c:/source/vanaheim/DDTs/discarded/2024_01_DDT_0001_0267_P005.pdf)
                        discarded_doc = discard_doc(working_doc, working_page)
                        # discarded_doc_name: basename del doc di scarto (es. 2024_01_DDT_0001_0267_P005.pdf)
                        discarded_doc_name = discarded_doc.split('/')[-1]

                        logger.error(f"discarding page {working_page} of {working_doc_name} for error on saving record from discard_consegne... [{discarded_doc_name}]")
                        if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                            'page': working_page,
                            'doc': working_doc_name,
                            'pattern': 'QUERY_INSERT_CONSEGNE from discard_consegne',
                            'numero_documento': doc_info['numero_documento'],
                            'genere_documento': doc_info['genere_documento'],
                            'data_documento': doc_info['data_documento']
                        })) != 1:
                            logger.error('error on saving discard message record...')
                    elif sqlmng.conx_write(cursor, QUERY_UPDATE_MESSAGGI, chk_discard.id_messaggio) != 1:
                        logger.error(f'error on update message status... [message id: {chk_discard.id_messaggio}]')
                else:
                    logger.warning(f'found NULL cell on saving consegne from discard_consegne in doc {working_doc_name}... skipping record!')
            else:
                discarded_pages['number'] += 1
                discarded_doc = discard_doc(working_doc, working_page)
                discarded_doc_name = discarded_doc.split('/')[-1]

                if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, discarded_pages['discard_message']) != 1:
                    logger.error('error on saving discard message record...')
                else:
                    doc_info['sorgente'] = discarded_doc_name
                    doc_info['id_messaggio'] = cursor.fetchone()[0]

                    if sqlmng.conx_write(cursor, QUERY_INSERT_DISCARD_CONSEGNE, [value for value in doc_info.values()]) != 1:
                        logger.error(f'error on saving discard_consegne record... [message id: {doc_info['id_messaggio']}]')

            discarded_pages['is_discarded'] = False
            continue

        doc_info['sorgente'] = working_doc_name
        doc_info['pagina'] = working_page
        doc_info['data_registrazione'] = recording_begin
        logger.info(doc_info)

        # controllo se è un duplicato
        chk_dup = sqlmng.conx_read(cursor, QUERY_CHK_DUPLICATE, (
            doc_info['sorgente'],
            doc_info['pagina'],
            doc_info['numero_documento'],
            doc_info['genere_documento'],
            doc_info['data_documento'].year
        )).fetchone()[0]

        if chk_dup:
            discarded_pages['number'] += 1
            discarded_doc = discard_doc(working_doc, working_page)
            discarded_doc_name = discarded_doc.split('/')[-1]

            logger.warning(f"discarding page {working_page} of {working_doc_name} because already recorded... [{discarded_doc_name}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                'page': working_page,
                'doc': working_doc_name,
                'pattern': 'QUERY_CHK_DUPLICATE',
                'numero_documento': doc_info['numero_documento'],
                'genere_documento': doc_info['genere_documento'],
                'data_documento': doc_info['data_documento']
            })) != 1:
                logger.error('error on saving discard message record...')
            continue
        elif sqlmng.conx_write(cursor, QUERY_INSERT_CONSEGNE, [value for value in doc_info.values()]) != 1:
            discarded_pages['number'] += 1
            discarded_doc = discard_doc(working_doc, working_page)
            discarded_doc_name = discarded_doc.split('/')[-1]

            logger.error(f"discarding page {working_page} of {working_doc_name} for error on saving record... [{discarded_doc_name}]")
            if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, (PATTERN_MESSAGE_DISCARD['genere'], PATTERN_MESSAGE_DISCARD['testo'] % {
                'page': working_page,
                'doc': working_doc_name,
                'pattern': 'QUERY_INSERT_CONSEGNE',
                'numero_documento': doc_info['numero_documento'],
                'genere_documento': doc_info['genere_documento'],
                'data_documento': doc_info['data_documento']
            })) != 1:
                logger.error('error on saving discard message record...')

        # controllo se bisogna aggiornare un gap message
        chk_gap = sqlmng.conx_read(cursor, QUERY_CHK_RECORD_GAP, (
            doc_info['numero_documento'],
            doc_info['data_documento'].year
        )).fetchone()
        if chk_gap and sqlmng.conx_write(cursor, QUERY_UPDATE_MESSAGGI, [chk_gap.id]) != 1:
            logger.error(f'error on update message status... [message id: {chk_gap.id}]')

    doc.close()
    return doc_pages, discarded_pages['number']


def discard_doc(working_doc: str, working_page: int) -> str:
    """Generate a new pdf document by extracting a single page from another pdf document.

    :param str working_doc: Path to the source pdf document.
    :param int working_page: Page number which must be extracted.
    :return: The name of the new document.
    """
    # working_doc: percorso con suffisso '.recording' (es. c:/source/vanaheim/DDTs/2024_01_DDT_0001_0267.pdf.recording)
    # working_page: numero di pagina in errore
    # doc: raw PDF doc
    doc = pypdfium2.PdfDocument(working_doc)
    # discard: nuovo raw PDF doc
    discard = pypdfium2.PdfDocument.new()

    discard.import_pages(doc, [working_page - 1])
    # discard_doc: percorso del nuovo doc (es. c:/source/vanaheim/DDTs/discarded/2024_01_DDT_0001_0267_P005.pdf)
    discard_doc = f"{PATH_DISCARDED_DIR}/{working_doc.split('/')[-1].split('.')[0]}_P{working_page:0>3}.pdf"
    discard.save(discard_doc)

    discard.close()
    doc.close()
    return discard_doc


def check_similarity(doc_targa: str) -> str:
    """Check the plate similarity with the plates in recording_doc.ENUM_TARGA list, in order to get the correct plate.

    :param str doc_targa: The starting plate.
    :return: The plate most similar, or the starting plate if the similarity index is lower than 50%.
    """
    logger = logger_ini(PATH_LOG, 'recording_doc')
    logger.info(f'checking similarity for {doc_targa}...')

    enum = dict.fromkeys(ENUM_TARGA, 0.0)
    for targa in enum:
        enum[targa] = SequenceMatcher(None, targa, doc_targa).ratio()

    max_score = sorted(enum, key=enum.get, reverse=True)[0]
    return max_score if enum[max_score] > 0.5 else doc_targa


if __name__ == '__main__':
    logger = logger_ini(PATH_LOG, 'recording_doc')
    # docs: elenco dei doc in PATH_WORKING_DIR (es. [2024_01_DDT_0001_0267.pdf, ...])
    docs = sorted(next(os.walk(PATH_WORKING_DIR), (None, None, []))[2])
    logger.info(f'DDTs dir content {docs}')

    recording_begin = datetime.now()
    cursor = sqlmng.conx_ini(save_changes=True)
    # doc: un singolo doc dell'elenco (es. 2024_01_DDT_0001_0267.pdf)
    for doc in docs:
        if not re.search(PATTERN_WORKING_DOC, doc):
            logger.info(f'error on PATTERN_WORKING_DOC for {doc}... skipping doc!')
            continue

        logger.info(f'working on doc {doc}...')
        # working_doc: percorso con suffisso '.recording' (es. c:/source/vanaheim/DDTs/2024_01_DDT_0001_0267.pdf.recording)
        working_doc = f'{PATH_WORKING_DIR}/{doc}.recording'
        os.rename(f'{PATH_WORKING_DIR}/{doc}', working_doc)

        # worked_pages: numero totale di pagine di working_doc
        # discarded_pages: numero di pagine in errore in working_doc
        worked_pages, discarded_pages = doc_scanner(working_doc, cursor, recording_begin)

        logger.info(f'worked {worked_pages} pages on {doc} [{discarded_pages} discarded pages]')
        os.rename(working_doc, f'{PATH_RECORDED_DIR}/{doc}.recorded')

    # verifico gaps di numero_documento in vanaheim.consegne
    gaps = sqlmng.conx_read(cursor, QUERY_CHK_GAPS).fetchall()
    for row in gaps:
        if sqlmng.conx_write(cursor, QUERY_INSERT_MESSAGGI, (PATTERN_MESSAGE_GAPS['genere'], PATTERN_MESSAGE_GAPS['testo'] % {
            'numero_documento': row.numero,
            'anno': row.anno
        })) != 1:
            logger.error('error on saving gap message record...')

    # aggiorno overview dei doc registrati
    overviews = sqlmng.conx_read(cursor, QUERY_OVERVIEW_DATE, [recording_begin]).fetchall()
    for row in overviews:
        overview_gnr(row.anno, row.mese)

    cursor.close()
