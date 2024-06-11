## Requisitos
- `pandas`
```bash 
pip install pandas
```

- `duckdb`
```bash
pip install duckdb
```

- `openpyxl`
```bash
pip install openpyxl
```

## Estrutura do Script

### Classes e Métodos

#### Classe EtlLinkedin

Responsável pelo processamento ETL.

- Métodos
  - `__init__`(self, raw_directory, clean_directory): Inicializa a classe com diretórios de dados brutos e limpos.
  - `detect_file_category`(self, file): Detecta a categoria de um arquivo com base em seu nome.
  - `get_raw_files`(self, raw_directory): Detecta e retorna uma lista de arquivos brutos a serem processados.
  - `read_excel_file`(self, file): Lê um arquivo Excel e retorna seus dados como uma lista de DataFrames.
  - `extract_data`(self): Extrai os dados brutos dos arquivos e retorna uma lista de DataFrames.
  - `convert_dataframes_to_duckdb`(self, data): Converte os DataFrames Pandas em tabelas DuckDB.
  - `register_dataframe_in_duckdb`(self, dataframe): Registra um dataframe pandas em DuckDB.
  - `process_content_metrics`(self, table): Processa a tabela conteúdo_métrica.
  - `add_final_date`(self, table): Adiciona uma data final a tabela com base no período de extração.
  - `transform_data`(self, tables): Aplica uma série de transformações aos dados extraídos.
  - `load_to_clean`(self, tables): Carrega os dados transformados no diretório de dados limpos.
  - `concatenate_monthly_tables`(self, tables): Identifica e agrupa tabelas de mesma categoria e mesmo mês em uma lista.
  - `export_tables`(self, tables, export_type): Exporta um DataFrame concatenado para um arquivo CSV.
  - `concatenate_category_tables`(self, monthly_data): Identifica e agrupa tabelas de mesma categoria.

## Função Principal main

1. Inicializa a classe EtlLinkedin com os diretórios de dados brutos e limpos.
2. Extrai os dados brutos dos arquivos.
3. Converte os dataframes pandas para tabelas em DuckDB.
4. Aplica transformações aos dados extraídos.
5. Carrega os dados transformados no diretório de dados limpos.
6. Identifica e agrupa tabelas de mesma categoria e mês.
7. Exporta os DataFrames concatenados para arquivos CSV mensais.
8. Identifica e agrupa tabelas de mesma categoria.
9. Exporta os DataFrames concatenados por categoria para arquivos CSV finais.

## Como Executar
```bash

python etl_linkedin_duckdb.py
```

## Estrutura de Diretórios
- `data/linkedin/raw`: Diretório contendo os dados brutos. (ANO/MES)
- `data/linkedin/clean`: Diretório onde os dados limpos serão armazenados.(ANO/MES)
- `data/linkedin/clean`/concatenated_dataframes: Diretório onde os DataFrames concatenados serão armazenados.