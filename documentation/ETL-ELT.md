# Notes
copied from https://aws.amazon.com/it/compare/the-difference-between-etl-and-elt/


# Qual è la differenza tra ETL ed ELT?

Estrazione, trasformazione e caricamento (ETL) ed estrazione, caricamento e trasformazione (ELT) sono due approcci di elaborazione dei dati per l'analisi. Le grandi organizzazioni dispongono di diverse centinaia (o addirittura migliaia) di origini dati relative a tutti gli aspetti delle loro operazioni, come applicazioni, sensori, infrastruttura IT e partner di terze parti. Devono filtrare, ordinare e pulire questo grande volume di dati per renderlo utile per l'analisi e la business intelligence. L'approccio ETL utilizza una serie di regole aziendali per elaborare i dati da diverse origini prima dell'integrazione centralizzata. L'approccio ELT carica i dati così come sono e li trasforma in una fase successiva, a seconda del caso d'uso e dei requisiti di analisi. Il processo ETL richiede maggiore definizione all'inizio. È infatti necessario coinvolgere fin da subito l'analisi per definire tipi, strutture e relazioni dei dati di destinazione. I data scientist sfruttano l'ETL principalmente per caricare i database legacy nel data warehouse e, a oggi, l'ELT è ormai una procedura standard.


# Quali sono le similitudini tra ETL ed ELT?

Sia estrazione, trasformazione e caricamento (ETL) che estrazione, caricamento e trasformazione (ELT) sono sequenze di processi che preparano i dati per ulteriori analisi. Acquisiscono, elaborano e caricano i dati per l'analisi in tre fasi. 

## Estrazione

L'estrazione è la prima fase sia dell'ETL che dell'ELT. Questo passaggio riguarda la raccolta di dati non elaborati da diverse origini. Queste possono essere database, file, applicazioni software come servizio (SaaS), sensori Internet delle cose (IoT) o eventi applicativi. In questa fase puoi raccogliere dati semistrutturati, strutturati o non strutturati.

## Trasformazione

Nel processo ETL, la trasformazione è la seconda fase, mentre nell'ELT è la terza. Questo passaggio si concentra sulla modifica dei dati non elaborati dalla loro struttura originale in un formato che soddisfi i requisiti del sistema di destinazione in cui si prevede di archiviare i dati per l'analisi. Ecco alcuni esempi di trasformazione:

- Modifica dei tipi o dei formati di dati
- Rimozione di dati non coerenti o imprecisi.
- Rimozione della duplicazione dei dati.

Applichi regole e funzioni per pulire e preparare i dati per l'analisi nel sistema di destinazione.

## Caricamento

In questa fase, i dati vengono archiviati nel database di destinazione. ETL elabora i dati di caricamento come fase finale, in modo che gli strumenti di reporting possano utilizzarli direttamente per generare report e approfondimenti utilizzabili. Tuttavia, in ELT, è ancora necessario trasformare i dati estratti dopo averli caricati.

# In che modo i processi ELT ed ETL differiscono l'uno dall'altro?

Di seguito descriviamo i processi di estrazione, trasformazione e caricamento (ETL) ed estrazione, caricamento e trasformazione (ELT). Puoi anche leggere alcuni retroscena storici.

## Processo ETL

ETL prevede tre fasi:

 - Estrazione dei dati non elaborati da varie origini
 - Utilizzo di un server di elaborazione secondario per trasformare tali dati
 - Caricamento dei dati in un database di destinazione

La fase di trasformazione garantisce la conformità ai requisiti strutturali del database di destinazione. I dati vengono spostati solo dopo che sono stati trasformati e sono pronti.

 
## Processo ELT

Queste sono le tre fasi dell'ELT:

 - Estrazione dei dati non elaborati da varie origini
 - Caricamento dei dati allo stato naturale in un data warehouse o in un data lake
 - Trasformazione in base alle necessità mentre ci si trova nel sistema di destinazione

Con ELT, la pulizia, la trasformazione e l'arricchimento dei dati avvengono all'interno del data warehouse. Puoi interagire e trasformare i dati non elaborati tutte le volte che è necessario.
Storia di ETL ed ELT

L'ETL esiste dagli anni '70 ed è diventato particolarmente popolare con l'avvento dei data warehouse. Tuttavia, i data warehouse tradizionali richiedevano processi ETL personalizzati per ciascuna origine dati.

L'evoluzione delle tecnologie cloud ha cambiato ciò che era possibile. Le aziende possono ora archiviare un numero illimitato di dati non elaborati su larga scala e analizzarli in un secondo momento, se necessario. L'ELT è diventato il moderno metodo di integrazione dei dati per analisi efficienti.
