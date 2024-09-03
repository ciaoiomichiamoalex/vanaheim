CREATE SCHEMA vanaheim;

CREATE TABLE vanaheim.consegne (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    numero_documento INTEGER NOT NULL,
    genere_documento CHAR(2) NOT NULL,
    data_documento DATE NOT NULL,
    ragione_sociale VARCHAR(255) NOT NULL,
    sede_consegna VARCHAR(255) NOT NULL,
    quantita INTEGER NOT NULL,
    data_consegna DATE NOT NULL,
    targa CHAR(7) NOT NULL,
    sorgente VARCHAR(255),
    pagina INTEGER,
    data_registrazione TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT consegne_id_pk PRIMARY KEY (id),
    CONSTRAINT consegne_numero_documento_chk CHECK (numero_documento > 0),
    CONSTRAINT consegne_quantita_chk CHECK (quantita > 0),
    CONSTRAINT consegne_pagina_chk CHECK (pagina > 0)
);

CREATE INDEX consegne_ragione_sociale_idx ON vanaheim.consegne (ragione_sociale);
CREATE INDEX consegne_sede_consegna_idx ON vanaheim.consegne (sede_consegna);

CREATE TABLE vanaheim.messaggi (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    genere VARCHAR(255) NOT NULL,
    testo VARCHAR(255) NOT NULL,
    data_messaggio TIMESTAMP NOT NULL DEFAULT NOW(),
    stato BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT messaggi_id_pk PRIMARY KEY (id)
);

CREATE INDEX messaggi_testo_idx ON vanaheim.messaggi (testo);

CREATE OR REPLACE VIEW vanaheim.messaggi_discard_vw AS
SELECT NULLIF(SUBSTRING(
        testo,
        STRPOS(testo, '[numero: ') + LENGTH('[numero: '),
        STRPOS(testo, ', genere: ') - (STRPOS(testo, '[numero: ') + LENGTH('[numero: '))
    ), 'None')::INTEGER numero_documento,
    NULLIF(SUBSTRING(
        testo,
        STRPOS(testo, ', genere: ') + LENGTH(', genere: '),
        STRPOS(testo, ', data: ') - (STRPOS(testo, ', genere: ') + LENGTH(', genere: '))
    ), 'None') genere_documento,
    NULLIF(SUBSTRING(
        testo,
        STRPOS(testo, ', data: ') + LENGTH(', data: '),
        STRPOS(testo, ']') - (STRPOS(testo, ', data: ') + LENGTH(', data: '))
    ), 'None')::DATE data_documento,
    SUBSTRING(
        testo,
        STRPOS(testo, 'for error on ') + LENGTH('for error on '),
        STRPOS(testo, '[numero: ') - (STRPOS(testo, 'for error on ') + LENGTH('for error on '))
    ) errore,
    SUBSTRING(
        testo,
        STRPOS(testo, 'of doc ') + LENGTH('of doc '),
        STRPOS(testo, ' discarded') - (STRPOS(testo, 'of doc ') + LENGTH('of doc '))
    ) sorgente,
    SUBSTRING(
        testo,
        STRPOS(testo, 'Page ') + LENGTH('Page '),
        STRPOS(testo, ' of doc ') - (STRPOS(testo, 'Page ') + LENGTH('Page '))
    )::INTEGER pagina
FROM vanaheim.messaggi
WHERE genere = 'DISCARD';

CREATE OR REPLACE VIEW vanaheim.messaggi_gap_vw AS
SELECT id,
    SUBSTRING(
        testo,
        STRPOS(testo, 'doc number ') + LENGTH('doc number '),
        STRPOS(testo, ' of year ') - (STRPOS(testo, 'doc number ') + LENGTH('doc number '))
    )::INTEGER numero_documento,
    SUBSTRING(
        testo,
        STRPOS(testo, ' of year ') + LENGTH(' of year '),
        LENGTH(testo)
    )::INTEGER anno
FROM vanaheim.messaggi
WHERE genere = 'GAP';

CREATE OR REPLACE VIEW vanaheim.consegne_gap_vw AS
WITH doc_nums AS (
    SELECT dn.anno, s.numero
    FROM (
        SELECT EXTRACT(YEAR FROM data_documento) anno,
            MIN(numero_documento) min_num,
            MAX(numero_documento) max_num
        FROM vanaheim.consegne
        GROUP BY EXTRACT(YEAR FROM data_documento)
    ) dn,
        GENERATE_SERIES(dn.min_num, dn.max_num, 1) s(numero)
)
SELECT d.numero,
    d.anno,
    CASE
        WHEN m.numero_documento IS NOT NULL THEN TRUE
        ELSE FALSE
    END discarded
FROM doc_nums d
    LEFT JOIN vanaheim.consegne c
        ON d.numero = c.numero_documento
        AND d.anno = EXTRACT(YEAR FROM c.data_documento)
    LEFT JOIN vanaheim.messaggi_discard_vw m
        ON d.numero = m.numero_documento
        AND d.anno = EXTRACT(YEAR FROM m.data_documento)
WHERE c.numero_documento IS NULL;
