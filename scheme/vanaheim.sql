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
