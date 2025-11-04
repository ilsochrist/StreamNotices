# 🚀 Data Lakehouse Escalável e Seguro (Streaming & Medallion Architecture)

## 🌟 Visão Geral do Projeto

Este projeto pessoal complexo demonstra o desenvolvimento de uma **Arquitetura Data Lakehouse de ponta a ponta**, robusta, escalável e segura. O foco foi no processamento de **dados de *streaming***, garantindo pontualidade e integridade da informação.

A arquitetura foi cuidadosamente desenvolvida para transformar dados brutos em *insights* de alto valor, utilizando o padrão de camadas da indústria para excelência em Engenharia de Dados.

## 💡 Stack Tecnológico (Data Engineering)

| Categoria | Ferramenta | Uso no Projeto |
| :--- | :--- | :--- |
| **Plataforma/Computação** | Databricks | Desenvolvimento, execução (Jobs de ETL/ELT) e orquestração do pipeline de dados. |
| **Data Lakehouse** | Delta Lake | Armazenamento de dados, garantindo confiabilidade, *schema enforcement* e transações **ACID**. |
| **Segurança/Credenciais** | Variáveis de Ambiente (*env key*) | Autenticação segura do *workspace* e gerenciamento de segredos para acesso a recursos externos. |
| **Consumo/Visualização** | Power BI | *Frontend* de BI para visualização da camada **Gold** e tomada de decisão. |
| **Linguagens** | Python / SQL | Implementação dos processos de transformação (ETL/ELT). |

## 🏗️ Arquitetura Medallion: Integridade e Rastreabilidade

O pipeline foi estruturado seguindo o padrão **Medallion Architecture** (Bronze, Silver, Gold), garantindo rastreabilidade e integridade em cada etapa:

### 🥉 Camada Bronze (Raw)
* **Função:** Ingestão de dados brutos e imutáveis (*immutable data*) diretamente da fonte de *streaming*.
* **Características:** Mínima transformação (apenas metadados de ingestão), preservando o registro original.

### 🥈 Camada Silver (Cleaned & Enriched)
* **Função:** Aplicação de **Regras de Qualidade de Dados**, limpeza, padronização e enriquecimento do conjunto de dados.
* **Processos:** Tratamento de nulos, remoção de duplicatas e estruturação.
* **Análise de Sentimento:** Implementação de modelos de *Text Analysis* para extrair e persistir *scores* de sentimento, transformando dados textuais em métricas acionáveis.

### 🥇 Camada Gold (Curated & Consumable)
* **Função:** Dados modelados e otimizados (ex: *Star Schema*) para consumo de BI e *Machine Learning*.
* **Características:** Alta performance, estruturas consolidadas e prontas para exposição no Power BI, garantindo que as decisões de negócio sejam baseadas em dados curados.

## 🛡️ Segurança e Boas Práticas (Best Practices)

A segurança foi um pilar desde o desenvolvimento:

* **Isolamento de Credenciais:** O acesso ao Databricks foi autenticado com sucesso utilizando **variáveis de ambiente (`env key`)**, seguindo rigorosamente as melhores práticas de gerenciamento de segredos.
* **Controle de Acesso:** Garantia de conexão segura e isolamento de acessos entre ambientes (desenvolvimento/produção).

## 📊 Qualidade de Dados e Monitoramento

Métricas analíticas avançadas e indicadores de monitoramento foram desenvolvidos para garantir a saúde do dado:

| Métrica de Monitoramento | Descrição | Valor Analítico |
| :--- | :--- | :--- |
| **Latência de Ingestão** | Tempo de processamento dos dados de *streaming*. | Garante a pontualidade da informação para análise em tempo real. |
| **Top Keywords** | Extração de termos mais frequentes nos dados textuais. | Análise de tendências e *feedback* do usuário em tempo real. |
| **Scores de Sentimento** | Classificação do tom emocional do texto. | Base para **análise preditiva** de comportamento do cliente ou produto. |

## 🔗 Do Data Lakehouse ao Insight

O projeto cumpre a missão de conectar a **Engenharia de Dados de Alto Nível** ao **Valor de Negócio**, transformando dados de *streaming* complexos em um *dashboard* de alta qualidade no Power BI.

O resultado é um sistema robusto que permite que a empresa tome decisões baseadas em informações confiáveis, atuais e ricas em *insights* de **Análise Preditiva**.

---
