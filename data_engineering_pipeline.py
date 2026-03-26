# ===========================================================
# ESAME PRATICO M2: DATA ENGINEERING PIPELINE
# Requisiti: Python, Pandas, Dask, PySpark, Seaborn/Matplotlib
# Azienda: MegaShop
# Autore: Alfio Russo
# ===========================================================

import os
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
import pyarrow.parquet as pq
import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

import seaborn as sns
import matplotlib.pyplot as plt



# ===========================================================
# ===================== Esercizio 1 =========================
# Ingestion e Limiti di Memoria: Pandas vs Dask
# ===========================================================

json_dir = "./data_local/json"

# --- 1. Pandas: leggo file uno alla volta e sommo ---
print("=== Esercizio 1: Ingestion con Pandas ===")
total_amount = 0
for file in os.listdir(json_dir):
    if file.endswith(".jsonl"):
        df = pd.read_json(os.path.join(json_dir, file), lines=True)
        total_amount += df["amount"].sum()
        print(f"{file} processato")
print("Totale vendite (Pandas):", total_amount)

# --- 2. Dask: leggo tutti i file insieme e calcolo la media per regione ---
print("\n=== Esercizio 1: Ingestion con Dask ===")
df_dask = dd.read_json(os.path.join(json_dir, "*.jsonl"), lines=True)
mean_amount = df_dask.groupby("region_id")["amount"].mean()
print("Media amount per region_id (Dask):")
print(mean_amount.compute())



# ===========================================================
# ===================== Esercizio 2 =========================
# Pipeline ETL con PySpark
# ===========================================================

# --- 2.1 Correzione timestamp NANOS con PyArrow ---
parquet_files = [
    "./data_local/parquet/transactions_batch_0000.parquet",
    "./data_local/parquet/transactions_batch_0001.parquet",
    "./data_local/parquet/transactions_batch_0002.parquet",
    "./data_local/parquet/transactions_batch_0003.parquet",
    "./data_local/parquet/transactions_batch_0004.parquet"
]

parquet_fixed_dir = "./data_local/parquet_fixed"
Path(parquet_fixed_dir).mkdir(exist_ok=True)

print("\n=== Esercizio 2: Correzione file Parquet ===")
for f in parquet_files:
    table = pq.read_table(f)
    for col_name in table.schema.names:
        if pa.types.is_timestamp(table.schema.field(col_name).type):
            table = table.set_column(
                table.schema.get_field_index(col_name),
                col_name,
                table[col_name].cast(pa.string())
            )
    pq.write_table(table, Path(parquet_fixed_dir) / Path(f).name)
print("Correzione completata!")

# --- 2.2 Lettura Spark e join ---
spark = SparkSession.builder.appName("MegaShopETL").getOrCreate()
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Lettura transazioni corrette
df = spark.read.parquet(parquet_fixed_dir)

# Lettura tabelle prodotti e regioni
df_products = spark.read.parquet("./data_local/parquet/products.parquet")
df_regions = spark.read.parquet("./data_local/parquet/regions.parquet")

# Join transazioni -> prodotti -> regioni
df = df.join(df_products, on="product_id", how="left") \
       .join(df_regions, on="region_id", how="left") \
       .select("transaction_id", "region_name", "category", "amount", "ts", "year")

# Convertiamo 'ts' in timestamp
df = df.withColumn("timestamp", to_timestamp(col("ts")))

# Mostro i primi 5 record
print("\n=== DataFrame finale (primi 5 record) ===")
df.show(5)

# Salvataggio finale partizionato per anno
processed_dir = "./data_local/processed_sales"
df.write.mode("overwrite").partitionBy("year").parquet(processed_dir)
print(f"\nETL completato! Dati salvati in: {processed_dir}")

# Definiamo final_df per esercizi successivi
final_df = df



# ===========================================================
# ===================== Esercizio 3 =========================
# Data Visualization (Reporting)
# ===========================================================

print("\n=== Esercizio 3: Data Visualization ===")

# Aggregazione: fatturato totale per categoria
category_sales = final_df.groupBy("category") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_amount")

# Conversione in Pandas per visualizzazione
pdf = category_sales.toPandas()

# Grafico a barre
plt.figure(figsize=(10,6))
sns.set_style("whitegrid")

# Palette più accattivante e numeri sulle barre
palette = sns.color_palette("Set2", n_colors=len(pdf))
ax = sns.barplot(data=pdf, x="category", y="total_amount",
                 hue="category", dodge=False, palette=palette, legend=False)

plt.title("Fatturato Totale per Categoria (MegaShop)", fontsize=16, fontweight='bold')
plt.xlabel("Categoria", fontsize=12)
plt.ylabel("Fatturato (€)", fontsize=12)
plt.xticks(rotation=45)

# Valori sopra le barre
for i, v in enumerate(pdf['total_amount']):
    ax.text(i, v + max(pdf['total_amount'])*0.01, f"{v:,.0f}", ha='center', fontweight='bold')

plt.tight_layout()
plt.savefig("fatturato_per_categoria.png", dpi=150)
plt.show()



# ===========================================================
# ===================== Esercizio 4 =========================
# Real-Time Streaming (Bonus)
# ===========================================================

print("\n=== Esercizio 4: Streaming in tempo reale (20s) ===")

# Riutilizzo SparkSession esistente
spark_stream = spark

# Definizione schema dei JSON
schema = "transaction_id string, customer_id long, product_id int, region_id int, quantity int, amount float, ts string, year int, month int"

# Lettura streaming dalla cartella JSON
stream_df = spark_stream.readStream \
    .schema(schema) \
    .json("./data_local/json/")

# Calcolo numero transazioni per regione
result = stream_df.groupBy("region_id").count()

# Output in console con checkpoint
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "./checkpoint_stream") \
    .start()

# Mantiengo lo streaming attivo per 20 secondi
query.awaitTermination(timeout=20)

# Chiusura automatica dello streaming
query.stop()
print("Streaming terminato correttamente.")
