## Requisitos
- `Polars`
```bash 
pip install polars

# Or for legacy CPUs without AVX2 support
pip install polars-lts-cpu
```

## Estrutura do Script

### Classes e Métodos

#### Classe EtlLinkedin

Responsável pelo processamento ETL.

- Métodos
    - `__init__`(self, raw_directory, clean_directory): Inicializa a classe com diretórios de dados brutos e limpos.
        detect_file_category(self, file): Detecta a categoria de um arquivo com base em seu nome.
    - `get_raw_files`(self, raw_directory): Detecta e retorna uma lista de arquivos brutos a serem processados.
    - `read_excel_file`(self, file): Lê um arquivo Excel e retorna seus dados como uma lista de DataFrames.
    - `extract_data`(self): Extrai os dados brutos dos arquivos e retorna uma lista de DataFrames.
    - `translate_cols`(self, dataframe): Traduza os nomes das colunas de um DataFrame para o inglês.
    - `add_final_date`(self, dataframe): Adiciona uma data final ao DataFrame com base no período de extração.
    - `convert_column_types`(self, dataframe): Converte colunas específicas do DataFrame para o tipo de dado adequado.
    - `clean_content_metrics_data`(self, dataframe): Limpa e processa os dados de métricas de conteúdo.
    - `transform_data`(self, data): Aplica uma série de transformações aos dados extraídos.
    - `load_to_clean`(self, data): Carrega os dados transformados no diretório de dados limpos.
    - `concatenate_monthly_dataframes`(self, data): Agrupa e concatena os DataFrames extraídos por mês.
    - `export_dataframes`(self, data, file_prefix): Exporta DataFrames concatenados para um arquivo CSV.
    - `concatenate_category_dataframes`(self, data): Concatena todos os arquivos concatenados mensalmente em arquivos únicos por categoria.

## Função Principal main

Executa as operações ETL LinkedIn.

- Define os diretórios de dados brutos e limpos.
- Inicializa a classe EtlLinkedin.
- Extrai, transforma e carrega os dados.
- Concatena e exporta os DataFrames mensais e por categoria.

## Como Executar
```bash

python etl_linkedin_polars.py
```

## Estrutura de Diretórios
- `data/linkedin/raw`: Diretório contendo os dados brutos. (ANO/MES)
- `data/linkedin/clean`: Diretório onde os dados limpos serão armazenados.(ANO/MES)
- `data/linkedin/clean`/concatenated_dataframes: Diretório onde os DataFrames concatenados serão armazenados.