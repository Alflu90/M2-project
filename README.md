# M2-project
Epicode: Master in Python, AI &amp; Machine Learning – Progetto finale Modulo 2 (MegaShop Sales Data Pipeline)

# ESAME PRATICO M2: DATA ENGINEERING PIPELINE

## Autore
Alfio Russo

## Descrizione
Progetto di **Data Engineering e Data Visualization** per il Master in Python, AI & Machine Learning.  
Permette di processare grandi dataset di vendite, creare pipeline ETL con PySpark, fare analisi con Pandas/Dask e visualizzare i risultati con Seaborn.

Il progetto include:

1. **Ingestion** dei dati JSON con Pandas e Dask (gestione memoria vs Big Data)
2. **Pipeline ETL** con PySpark e correzione timestamp Parquet
3. **Data Visualization**: grafico a barre del fatturato per categoria
4. **Streaming in tempo reale** (bonus)

## Tecnologie utilizzate
- Python 3.10.11
- Pandas, Dask
- PySpark
- PyArrow
- Seaborn, Matplotlib

## Struttura del progetto

```data_local/
├─ json/ # file JSON di transazioni
├─ parquet/ # file parquet originali
├─ parquet_fixed/ # file parquet corretti automaticamente
├─ processed_sales/ # output ETL finale (partizionato per anno)
generator.py # script che ha creato la cartella data_local ed i file al suo interno
data_engineering_pipeline.py # file Python con codice per l'esecuzione del progetto
requirements.txt # librerie necessarie
README_progetto_finale.md # documentazione progetto
```

## Setup e installazione

### 1. Creare un ambiente virtuale e attivarlo:

```
bash
python -m venv .venv
```

## Linux/macOS
source .venv/bin/activate

## Windows
.venv\Scripts\activate


### 2. Installare le dipendenze:
```
pip install -r requirements.txt
```

## File requirements.txt esempio:
pandas==2.1.0
dask==2026.3.1
pyarrow==12.0.1
pyspark==3.5.1
seaborn==0.12.2
matplotlib==3.8.1


# 3. Generare dati di esempio con generator.py (fornito dal docente).

## Esecuzione
Avviare lo script principale:
```
python data_engineering_pipeline.py
```

Cosa succede durante l’esecuzione:

### Esercizio 1 – Ingestion Pandas vs Dask
**Pandas**: legge file JSON uno alla volta e calcola il totale vendite.
**Dask**: legge tutti i file JSON in un colpo solo e calcola la media per region_id.

### Esercizio 2 – ETL con PySpark
**Correzione** timestamp NANOS nei file Parquet usando PyArrow.
**Join** tra transazioni, prodotti e regioni.
**Creazione DataFrame** finale con colonne:
transaction_id, region_name, category, amount, ts, year, timestamp.
**Salvataggio** dei dati in processed_sales/ partizionati per anno.

### Esercizio 3 – Data Visualization
**Aggregazione** fatturato totale per categoria.
Generazione **grafico a barre** (fatturato_per_categoria.png) con palette colorata e valori sopra le barre.

### Esercizio 4 – Streaming (Bonus)
**Monitoraggio in tempo reale** della cartella data_local/json/.
Aggiornamento console con numero transazioni per regione.
**Streaming** limitato a 20 secondi per demo (timeout modificabile).


# Note e consigli
Lo streaming usa checkpoint automatico per evitare perdita dati; puoi rimuovere timeout per esecuzione continua.
Warning tipo:

```
WARN SparkSession: Using an existing Spark session...

WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming
```

sono normali e non influenzano l’esecuzione.

Tutti i path sono relativi: il progetto può essere eseguito su qualsiasi macchina.
Il grafico generato è salvato in PNG nella cartella corrente; puoi modificarne colori o stile con Seaborn/Matplotlib.
Cartelle pesanti come parquet_fixed/ o processed_sales/ possono essere ignorate nel repo usando **.gitignore**.


## Output di esempio
Grafico a barre: fatturato_per_categoria.png
Console Dask: media vendite per regione
Console Streaming: conteggio transazioni per regione in tempo reale (demo 20s)

# Licenza
Progetto creato per scopi didattici.
