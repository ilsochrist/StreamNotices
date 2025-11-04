from pyspark.sql.functions import *
from pyspark.sql.types import *

silver_table_name = "workspace.default.silver_news_cleaned"

# Nomes dos 4 Data Marts de Saída (Gold)
gold_table_name_by_source = "workspace.default.gold_sentiment_by_source"
gold_table_name_by_time = "workspace.default.gold_sentiment_by_time" 
gold_table_name_combined = "workspace.default.gold_source_by_time"
gold_table_name_final = "workspace.default.gold_final_dashboard_view" # A NOVA!

# --- 2. Ler a Tabela Silver ---
print(f"Lendo dados da Silver: {silver_table_name}")
silver_df = spark.read.table(silver_table_name)

if silver_df.isEmpty():
    print("A Tabela Silver está vazia. Não há dados para agregar.")
else:
    

    # --- 1: Agregação por Fonte e Sentimento ---
    print(f"\n[DM 1] Gerando Data Mart: {gold_table_name_by_source}")
    gold_df_source = (silver_df
        .groupBy("source_name", "sentiment_label")
        .agg(
            count("*").alias("total_artigos"),
            round(avg("sentiment_score"), 4).alias("media_sentimento_score")
        )
        .filter(col("source_name").isNotNull())
        .orderBy(desc("total_artigos"))
    )
    
    spark.sql(f"DROP TABLE IF EXISTS {gold_table_name_by_source}")
    (gold_df_source.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table_name_by_source)
    )
    print(f"Data Mart '{gold_table_name_by_source}' atualizado!")

    # --- 2: Agregação por Tempo (Hora e Dia da Semana) ---

    print(f"\n[DM 2] Gerando Data Mart: {gold_table_name_by_time}")
    
    gold_df_time = (silver_df
        .filter(col("published_hour").isNotNull())
        .groupBy("published_day_of_week", "published_hour")
        .agg(
            count("*").alias("total_artigos_por_hora"),
            round(avg("sentiment_score"), 4).alias("media_sentimento_horario")
        )
        .orderBy("published_day_of_week", "published_hour")
    )
    
    spark.sql(f"DROP TABLE IF EXISTS {gold_table_name_by_time}")
    (gold_df_time.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table_name_by_time)
    )
    print(f"Data Mart '{gold_table_name_by_time}' atualizado!")
    
    

    # --- 3: Agregação Combinada (Fonte e Hora) ---

    print(f"\n[DM 3] Gerando Data Mart: {gold_table_name_combined}")

    gold_df_combined = (silver_df
        .filter(col("published_hour").isNotNull())
        .groupBy("source_name", "published_hour")
        .agg(
            count("*").alias("total_artigos_combinados"),
            round(avg("sentiment_score"), 4).alias("media_sentimento_combinado")
        )
        .orderBy(desc("total_artigos_combinados"))
    )

    spark.sql(f"DROP TABLE IF EXISTS {gold_table_name_combined}")
    (gold_df_combined.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table_name_combined)
    )
    print(f"Data Mart '{gold_table_name_combined}' atualizado!")
    
    

    # --- 4: Visão Final do Dashboard (O Detalhe Completo) ---
    print(f"\n[DM 4] Gerando Tabela Final do Dashboard: {gold_table_name_final}")

    gold_df_final = (silver_df
        .select(
            # Colunas de Conteúdo Principal
            col("title").alias("artigo_titulo"),
            col("description").alias("artigo_descricao"),
            col("url").alias("link_original"),
            col("content").alias("artigo_conteudo_bruto"),

            # Colunas de Fonte e Tempo
            col("source_name").alias("fonte_nome"),
            col("author").alias("autor"),
            col("published_at").alias("data_publicacao"),
            col("published_hour").alias("hora_publicacao"),
            col("published_day_of_week").alias("dia_da_semana_cod"), 

            # Colunas de Sentimento e Análise de Texto
            col("sentiment_label").alias("sentimento_rotulo"),
            col("sentiment_score").alias("sentimento_score"),
            col("top_keywords").alias("palavras_chave_top5"),
            
            # Colunas de Engenharia de Dados e Qualidade
            col("complexity_level").alias("tamanho_nivel"),
            col("char_count").alias("tamanho_chars"),
            col("latency_seconds").alias("atraso_ingestao_seg")
        )
        .filter(col("title").isNotNull()) 
        .orderBy(desc("data_publicacao"))
    )

    spark.sql(f"DROP TABLE IF EXISTS {gold_table_name_final}")
    (gold_df_final.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table_name_final)
    )

    print(f"Data Mart Final '{gold_table_name_final}' atualizado com sucesso!")
    print("\nVisualização da Tabela Gold Final (Top 5):")
    display(spark.read.table(gold_table_name_final).limit(5))
