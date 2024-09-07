# VANAHEIM

1. Avviando `recording_doc.py` verranno registrati tutti i documenti PDF presenti in `vanaheim/DDTs/` che rispettano uno dei seguenti pattern:
	- `{AAAA}_{MM}_DDT_{NNNN}_{NNNN}.pdf`: documenti di un determinato anno/mese;
	- `{AAAA}_{MM}_DDT_{NNNN}_{NNNN}_P{NNN}.pdf`: scarto di un documento originario;
2. Vengono estratte le seguenti informazioni da ogni pagina di ogni documento:
	- *Numero documento*;
	- *Genere documento*;
	- *Data documento*;
	- *Ragione sociale*;
	- *Sede consegna*;
	- *Quantità*;
	- *Targa*;
3. In caso l'estrazione di una delle precedenti informazioni fallisca verrà generato uno scarto, estraendo quindi la pagina in `vanaheim/DDTs/discarded/` con il suffisso `_P{NNN}`; viene inoltre aggiunto un record in `vanaheim.discard_consegne`;
4. Prima di effettuare il salvataggio delle informazioni ottenute in `vanaheim.consegne` viene effettuato un controllo di univocità del record in modo da evitare duplicati;
5. Viene infine eseguito `overview_doc.py` che genera un Excel riassuntivo di ogni mese registrato in quella sessione.
