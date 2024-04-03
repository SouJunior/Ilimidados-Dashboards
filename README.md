# Ilimidados-Dashboards-WebScraping

Este é um projeto Python que realiza a extração de relatórios de estatísticas da página SouJunior no Linkedin.

## Estrutura do Projeto

- `linkedin`: Contém o módulo `webscraping_linkedin.py` que define a classe `ScraperLinkedin`, o arquivo `etl_linkedin.ipynb` que contém o fluxo de testes para o script de etl, a pasta `data` com a estrutura de dados extraídos crús e tratados.
- `selenium_env_tests`: Contém os testes de inicialização e de download de driver do Selenium.
- `app.py`: Arquivo principal que executa o fluxo do projeto.

## Requisitos

- Python 3.6 ou superior
- Bibliotecas: `selenium` e `webdriver-manager`

## Configuração

1. Clone o repositório no seu diretório de trabalho.
2. Instale as dependências executando `pip install -r requirements.txt`
3. Configure as variáveis de ambiente:

    ```bash
    LINKEDIN_EMAIL="email"
    LINKEDIN_PASSWORD="password"
    LINKEDIN_COMPANY_CODE="company_code"
    ```

## Uso

Execute o script `app.py` para realizar o download dos relatórios.