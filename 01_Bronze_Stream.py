from pyspark.sql.types import *
from pyspark.sql.functions import *

# --- 1. Definir os caminhos
source_path = "/Volumes/workspace/default/news_stream_source/"
checkpoint_path = "/Volumes/workspace/default/checkpoints/bronze_news_raw"
bronze_table_name = "workspace.default.bronze_news_raw"

# --- 2. Limpar
print(f"Limpando checkpoint em: {checkpoint_path}")
dbutils.fs.rm(checkpoint_path, recurse=True)
print(f"Derrubando tabela antiga: {bronze_table_name}")
spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")

# --- 3. Definir o Schema ---
source_schema = StructType([
    StructField("source", StructType([StructField("name", StringType())])),
    StructField("author", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("url", StringType()),
    StructField("publishedAt", StringType()),
    StructField("content", StringType())
])

# --- 4. Ler o Stream ---
print(f"Iniciando stream da fonte: {source_path}")
raw_df = (spark.readStream
    .schema(source_schema)
    .format("json")
    .option("maxFilesPerTrigger", 5) 
    .load(source_path)
)

# --- 5. Adicionar Metadados ---
bronze_df = raw_df.withColumn("ingestion_tms", current_timestamp())

# --- 6. Escrever o Stream na Tabela Delta "bronze_news_raw" ---
(bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path) 
    
    .trigger(availableNow=True) # Processa o que estiver disponível e para.
    
    .table(bronze_table_name)
)
