"""CREATE VOLUME IF NOT EXISTS workspace.default.news_stream_source;
"""
%pip install newsapi-python
dbutils.library.restartPython()


import json
from newsapi import NewsApiClient

import os
# --- Configuração ---
NEWS_API_KEY = dbutils.secrets.get(scope="news_app_scope", key="newsapi_key")

if NEWS_API_KEY:
    print("Chave API carregada com sucesso do Databricks Secrets.")

else:
    print("Erro ao carregar o segredo.")

# Apontado para o Volume dentro do catálogo 'workspace'
source_path = "/Volumes/workspace/default/news_stream_source/"

print(f"Limpando o diretório de origem: {source_path}")
# Limpa o diretório para começar do zero
dbutils.fs.rm(source_path, recurse=True)
dbutils.fs.mkdirs(source_path)
print("Diretório limpo e recriado.")

# --- Lógica da API ---
try:
    newsapi = NewsApiClient(api_key=newsapi_key)
    all_articles = newsapi.get_everything(q='tecnologia',
                                          language='pt',
                                          sort_by='publishedAt',
                                          page_size=20)

    print(f"Total de artigos encontrados: {all_articles['totalResults']}")

    # --- Simulação do Stream ---
    for i, article in enumerate(all_articles['articles']):
        clean_timestamp = article['publishedAt'].replace(":", "-")
        file_name = f"article_{i}_{clean_timestamp}.json"
        
        # O caminho completo para o arquivo no Volume
        file_path = f"{source_path}{file_name}"

        dbutils.fs.put(file_path, json.dumps(article), overwrite=True)

    print(f"{len(all_articles['articles'])} arquivos JSON escritos em {source_path}")

except Exception as e:
    print(f"Erro ao buscar notícias: {e}")
    fake_data = {"source": {"id": None, "name": "Fake News"}, "author": "Ilso", "title": "Databricks é legal", "description": "Um teste de pipeline.", "url": "http://fake.com", "publishedAt": "2025-01-01T00:00:00Z"}
    dbutils.fs.put(f"{source_path}fake_article.json", json.dumps(fake_data), overwrite=True)
    print("Arquivo falso criado para teste.")

