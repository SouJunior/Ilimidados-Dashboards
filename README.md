# Ilimidados-Dashboards-Etl

Este é um projeto Python que realiza o tratamento dos arquivos brutos extraídos do linkedin para o formato .csv para ser consumido pelo BI

## Como utilizar


- Faça o download e extração do conteúdo do repositorio
- Crie um ambiente virtual

```bash
python -m venv .venv
```

- Ative o ambiente virtual

```bash
.venv/scripts/activate       
```

- Instale as dependências
  
```bash
pip install -r requirements.txt
```

- Execute o script uma primeira vez para criar as estruturas de pastas

```bash
python app.py
```

- Coloque os arquivos de extrações em pastas agrupadas por range de extração em `linkedin/data/raw/365d`
- Execute o script novamente para realizar a transformação dos arquivos em .csv

> Arquivos processados estarão disponiveis em: ´linkedin/data/processed´