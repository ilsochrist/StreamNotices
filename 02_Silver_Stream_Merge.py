%pip install textblob delta-spark nltk

# Importa NLTK para baixar o dicionário
import nltk
try:
    # Baixa o 'punkt' (tokenizador) e as 'stopwords' (apenas na primeira execução)
    nltk.download('punkt')
    nltk.download('stopwords')
except:
    pass # Ignora se já estiver baixado

# Reinicia o ambiente para que as bibliotecas sejam carregadas corretamente
dbutils.library.restartPython()

from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
from delta.tables import DeltaTable 
from pyspark.sql.window import Window 
import delta

# Para a UDF de Keywords
from nltk.corpus import stopwords
from collections import Counter
import re


# --- 1. Definir os caminhos e Nomes ---
checkpoint_path = "/Volumes/workspace/default/checkpoints/silver_news"
bronze_table_name = "workspace.default.bronze_news_raw"
silver_table_name = "workspace.default.silver_news_cleaned"


# --- 2. Limpar (para poder rodar com o novo schema) ---
print(f"Limpando checkpoint em: {checkpoint_path}")
dbutils.fs.rm(checkpoint_path, recurse=True)

print(f"Derrubando tabela antiga: {silver_table_name}")
spark.sql(f"DROP TABLE IF EXISTS {silver_table_name}")


# --- 3. Criar a tabela Silver vazia (com NOVO SCHEMA) ---
print(f"Recriando tabela: {silver_table_name}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_table_name} (
        source_name STRING,
        author STRING,
        title STRING,
        description STRING,
        url STRING,
        published_at TIMESTAMP,
        content STRING,
        sentiment_score FLOAT,
        sentiment_label STRING,
        
        -- NOVAS COLUNAS
        latency_seconds BIGINT, 
        published_hour INT,
        published_day_of_week INT,
        char_count INT,
        complexity_level STRING,
        top_keywords ARRAY<STRING>
    ) USING DELTA
""")

# --- 4. Definir as UDFs ---

# UDF de Análise de Sentimento
def get_sentiment(text):
    if text:
        try:
            try:
                blob_en = TextBlob(text).translate(from_lang='pt', to='en')
            except:
                blob_en = TextBlob(text) 
                
            score = blob_en.sentiment.polarity
            if score > 0.1:
                return (score, "Positivo")
            elif score < -0.1:
                return (score, "Negativo")
            else:
                return (score, "Neutro")
        except Exception: 
            return (0.0, "Indeterminado")
    return (0.0, "Indeterminado")

sentiment_udf = udf(get_sentiment, 
                    StructType([
                        StructField("score", FloatType()),
                        StructField("label", StringType())
                    ]))

# UDF para Extração de Palavras-Chave
STOPWORDS_PT = set(stopwords.words('portuguese'))
STOPWORDS_ADICIONAIS = {'o', 'a', 'e', 'da', 'de', 'do', 'em', 'para', 'com', 's'}
STOPWORDS_COMPLETA = STOPWORDS_PT.union(STOPWORDS_ADICIONAIS)

def get_top_keywords(text, top_n=5):
    if not text:
        return []
    
    text = text.lower()
    tokens = re.findall(r'\b[a-z]{3,}\b', text) 
    filtered_tokens = [w for w in tokens if w not in STOPWORDS_COMPLETA]
    counts = Counter(filtered_tokens)
    
    return [word for word, count in counts.most_common(top_n)]

keywords_udf = udf(get_top_keywords, ArrayType(StringType()))


# --- 5. Ler o Stream da Tabela Bronze ---
print(f"Iniciando stream da tabela: {bronze_table_name}")
silver_stream_df = spark.readStream.table(bronze_table_name)

# --- 6. Aplicar as transformações (Enriquecimento) ---
transformed_df = (silver_stream_df
    .withColumn("source_name", col("source.name"))
    .withColumn("published_at", to_timestamp(col("publishedAt")))
    
    # Sentimento
    .withColumn("sentiment_struct", sentiment_udf(col("description")))
    .withColumn("sentiment_score", col("sentiment_struct.score"))
    .withColumn("sentiment_label", col("sentiment_struct.label"))
    
    # 2A. Atraso de Ingestão (Latency)
    .withColumn("latency_seconds", 
        (unix_timestamp(col("ingestion_tms")) - unix_timestamp(col("published_at")))
    )
    
    # 2B. Features de Tempo
    .withColumn("published_hour", hour(col("published_at")))
    .withColumn("published_day_of_week", dayofweek(col("published_at")))
    
    # 1B. Nível de Complexidade
    .withColumn("char_count", length(col("content")))
    .withColumn("complexity_level", 
        when(col("char_count").isNull(), lit("Desconhecido"))
        .when(col("char_count") < 500, lit("Curta"))
        .when(col("char_count").between(500, 1500), lit("Média"))
        .otherwise(lit("Longa"))
    )
    
    # 1A. Extração de Entidades (Top Tags)
    .withColumn("top_keywords", keywords_udf(col("title"))) 

    .select("source_name", "author", "title", "description", "url", 
            "published_at", "content", 
            "sentiment_score", "sentiment_label",
            "latency_seconds", "published_hour", "published_day_of_week", 
            "char_count", "complexity_level", "top_keywords" 
    )
)

# --- 7. Definir a Lógica de MERGE (Upsert) ---
def upsert_to_silver(micro_batch_df, batch_id):
    
    window = Window.partitionBy("url", "published_at").orderBy(lit(1)) 
    deduped_df = micro_batch_df.withColumn("rank", rank().over(window)).filter("rank = 1").drop("rank")

    (DeltaTable.forName(spark, silver_table_name).alias("target")
        .merge(
            deduped_df.alias("source"),
            "target.url = source.url AND target.published_at = source.published_at" 
        )
        .whenNotMatchedInsertAll()
        .whenMatchedUpdateAll()
        .execute()
    )

# --- 8. Escrever o Stream usando foreachBatch ---
print("Escrevendo micro-batch na Silver...")

query = (transformed_df.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("update")
    .option("checkpointLocation", checkpoint_path) 
    .trigger(availableNow=True) 
    .start()
)
query.awaitTermination()

print("Processamento Silver concluído e query encerrada.")
