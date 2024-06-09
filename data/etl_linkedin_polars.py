import polars as pl
import os
import re

# import warnings

# warnings.simplefilter("ignore")


class EtlLinkedinPolars:
    """
    Classe responsável pelo processamento ETL (Extração, Transformação e Carga) de dados do LinkedIn.
    """

    def __init__(self, raw_directory, clean_directory):
        """
        Inicializa a classe LinkedInETLProcessor com os diretórios de dados brutos e limpos.

        Parâmetros:
        raw_directory (str): Diretório contendo os dados brutos.
        clean_directory (str): Diretório onde os dados limpos serão armazenados.
        """
        self.raw_directory = raw_directory
        self.clean_directory = clean_directory

    def detect_file_category(self, file):
        """
        Detecta a categoria de um arquivo com base em seu nome.

        Parâmetros:
        file (str): Nome do arquivo.

        Retorno:
        str: Categoria do arquivo (competitor, content, followers, visitors) ou 0 se não identificado.
        """
        if "competitor" in file:
            return "competitor"
        elif "content" in file:
            return "content"
        elif "followers" in file:
            return "followers"
        elif "visitors" in file:
            return "visitors"
        return 0

    def get_raw_files(self, raw_directory):
        """
        Detecta e retorna uma lista de arquivos brutos a serem processados.

        Parâmetros:
        raw_directory (str): Diretório contendo os dados brutos.

        Retorno:
        list: Lista de dicionários com informações sobre os arquivos brutos.
        """
        extraction_files = []
        for category in os.listdir(raw_directory):
            category_path = os.path.join(raw_directory, category)

            for year in os.listdir(category_path):
                year_path = os.path.join(category_path, year)

                for month in os.listdir(year_path):
                    month_path = os.path.join(year_path, month)

                    monthly_files = os.listdir(month_path)
                    if not monthly_files:
                        continue

                    for i, file in enumerate(monthly_files):
                        file_path = os.path.join(month_path, file)
                        df_category = self.detect_file_category(file)
                        extraction_files.append(
                            {
                                "category": df_category,
                                "file_path": file_path,
                                "dir": month_path,
                                "extraction_period": f"{year}-{month}-{i+1}",
                            }
                        )
        return extraction_files

    def read_excel_file(self, file):
        """
        Lê um arquivo Excel e retorna seus dados como uma lista de DataFrames.

        Parâmetros:
        file (dict): Dicionário com informações sobre o arquivo, incluindo categoria, caminho e período de extração.

        Retorno:
        list: Lista de dicionários contendo o nome do DataFrame, diretório, período de extração e o DataFrame.
        """
        category_keys = {
            "competitor": [{"sheet_name": "competitor", "sheet_pos": 1, "skiprows": 1}],
            "content": [
                {"sheet_name": "content_metrics", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "content_posts", "sheet_pos": 2, "skiprows": 0},
            ],
            "followers": [
                {"sheet_name": "followers_new", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "followers_location", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "followers_function", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "followers_experience", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "followers_industry", "sheet_pos": 5, "skiprows": 0},
                {"sheet_name": "followers_company_size", "sheet_pos": 6, "skiprows": 0},
            ],
            "visitors": [
                {"sheet_name": "visitors_metrics", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "visitors_location", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "visitors_function", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "visitors_experience", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "visitors_industry", "sheet_pos": 5, "skiprows": 0},
                {"sheet_name": "visitors_company_size", "sheet_pos": 6, "skiprows": 0},
            ],
        }

        sheets_to_read = category_keys[file["category"]]

        dataframes = []
        for sheet in sheets_to_read:
            # Original Pandas
            # df = pd.read_excel(
            #     file["file_path"],
            #     sheet_name=sheet["sheet_pos"],
            #     skiprows=sheet["skiprows"],
            # )

            df = pl.read_excel(
                source=file["file_path"],
                sheet_id=sheet["sheet_pos"],
                read_options={"skip_rows": sheet["skiprows"]},
            )

            if file["category"] == "content":
                first_row = df.row(0)
                df.columns = first_row
                df = df.slice(1, df.height)

            dataframes.append(
                {
                    "dataframe_name": sheet["sheet_name"],
                    "dir": file["dir"],
                    "extraction_period": file["extraction_period"],
                    "df": df,
                }
            )

        return dataframes

    def extract_data(self):
        """
        Extrai os dados brutos dos arquivos e retorna uma lista de DataFrames.

        Retorno:
        list: Lista de dicionários contendo os dados extraídos.
        """

        files = self.get_raw_files(self.raw_directory)
        data = [obj for file in files for obj in self.read_excel_file(file)]
        return data

    def translate_cols(self, dataframe):
        """
        Traduza os nomes das colunas de um DataFrame para o inglês.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os nomes das colunas traduzidos.
        """
        translated_columns = {
            "content_metrics": [
                "Date",
                "Impressions (organic)",
                "Impressions (sponsored)",
                "Impressions (total)",
                "Unique impressions (organic)",
                "Clicks (organic)",
                "Clicks (sponsored)",
                "Clicks (total)",
                "Reactions (organic)",
                "Reactions (sponsored)",
                "Reactions (total)",
                "Comments (organic)",
                "Comments (sponsored)",
                "Comments (total)",
                "Shares (organic)",
                "Shares (sponsored)",
                "Shares (total)",
                "Engagement rate (organic)",
                "Engagement rate (sponsored)",
                "Engagement rate (total)",
            ],
            "content_posts": [
                "Post Title",
                "Post Link",
                "Post Type",
                "Campaign Name",
                "Published by",
                "Date",
                "Campaign Start Date",
                "Campaign End Date",
                "Audience",
                "Impressions",
                "Views (excluding off-site video views)",
                "Off-site Views",
                "Clicks",
                "Click-Through Rate (CTR)",
                "Likes",
                "Comments",
                "Shares",
                "Followers",
                "Engagement Rate",
                "Content Type",
            ],
            "followers_new": [
                "Date",
                "Followers Sponsored",
                "Followers Organic",
                "Total Followers",
            ],
            "followers_location": ["Location", "Total Followers"],
            "followers_function": ["Function", "Total Followers"],
            "followers_experience": ["Experience Level", "Total Followers"],
            "followers_industry": ["Industry", "Total Followers"],
            "followers_company_size": ["Company Size", "Total Followers"],
            "visitors_metrics": [
                "Date",
                "Page Views Overview (Desktop)",
                "Page Views Overview (Mobile Devices)",
                "Page Views Overview (Total)",
                "Unique Visitors Overview (Desktop)",
                "Unique Visitors Overview (Mobile Devices)",
                "Unique Visitors Overview (Total)",
                "Page Views Day by Day (Desktop)",
                "Page Views Day by Day (Mobile Devices)",
                "Page Views Day by Day (Total)",
                "Unique Visitors Day by Day (Desktop)",
                "Unique Visitors Day by Day (Mobile Devices)",
                "Unique Visitors Day by Day (Total)",
                "Page Views Jobs (Desktop)",
                "Page Views Jobs (Mobile Devices)",
                "Page Views Jobs (Total)",
                "Unique Visitors Jobs (Desktop)",
                "Unique Visitors Jobs (Mobile Devices)",
                "Unique Visitors Jobs (Total)",
                "Total Page Views (Desktop)",
                "Total Page Views (Mobile Devices)",
                "Total Page Views (Total)",
                "Total Unique Visitors (Desktop)",
                "Total Unique Visitors (Mobile Devices)",
                "Total Unique Visitors (Total)",
            ],
            "visitors_location": ["Location", "Total Views"],
            "visitors_function": ["Function", "Total Views"],
            "visitors_experience": ["Experience Level", "Total Views"],
            "visitors_industry": ["Industry", "Total Views"],
            "visitors_company_size": ["Company Size", "Total Views"],
            "competitor": [
                "Page",
                "Total Followers",
                "New Followers",
                "Total Post Engagements",
                "Total Posts",
            ],
        }

        dataframe["df"].columns = translated_columns.get(dataframe["dataframe_name"])
        return dataframe

    def add_final_date(self, dataframe):
        """
        Adiciona uma data final ao DataFrame com base no período de extração.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com a data final adicionada.
        """
        map_months_period = {
            "2023-Jan-1": "01/15/2023",
            "2023-Jan-2": "01/31/2023",
            "2023-Fev-1": "02/15/2023",
            "2023-Fev-2": "02/28/2023",
            "2023-Mar-1": "03/15/2023",
            "2023-Mar-2": "03/31/2023",
            "2023-Abr-1": "04/15/2023",
            "2023-Abr-2": "04/30/2023",
            "2023-Maio-1": "05/15/2023",
            "2023-Maio-2": "05/31/2023",
            "2023-Jun-1": "06/15/2023",
            "2023-Jun-2": "06/30/2023",
            "2023-Jul-1": "07/15/2023",
            "2023-Jul-2": "07/31/2023",
            "2023-Ago-1": "08/15/2023",
            "2023-Ago-2": "08/31/2023",
            "2023-Set-1": "09/15/2023",
            "2023-Set-2": "09/30/2023",
            "2023-Out-1": "10/15/2023",
            "2023-Out-2": "10/31/2023",
            "2023-Nov-1": "11/15/2023",
            "2023-Nov-2": "11/30/2023",
            "2023-Dez-1": "12/15/2023",
            "2023-Dez-2": "12/31/2023",
            "2024-Jan-1": "01/15/2024",
            "2024-Jan-2": "01/31/2024",
            "2024-Fev-1": "02/15/2024",
            "2024-Fev-2": "02/29/2024",
            "2024-Mar-1": "03/15/2024",
            "2024-Mar-2": "03/31/2024",
            "2024-Abr-1": "04/15/2024",
            "2024-Abr-2": "04/30/2024",
            "2024-Maio-1": "05/15/2024",
            "2024-Maio-2": "05/31/2024",
            "2024-Jun-1": "06/15/2024",
            "2024-Jun-2": "06/30/2024",
            "2024-Jul-1": "07/15/2024",
            "2024-Jul-2": "07/31/2024",
            "2024-Ago-1": "08/15/2024",
            "2024-Ago-2": "08/31/2024",
            "2024-Set-1": "09/15/2024",
            "2024-Set-2": "09/30/2024",
            "2024-Out-1": "10/15/2024",
            "2024-Out-2": "10/31/2024",
            "2024-Nov-1": "11/15/2024",
            "2024-Nov-2": "11/30/2024",
            "2024-Dez-1": "12/15/2024",
            "2024-Dez-2": "12/31/2024",
        }

        final_date = map_months_period[dataframe["extraction_period"]]

        dataframe["df"] = dataframe["df"].with_columns(
            pl.lit(final_date).alias("Extraction Range")
        )
        return dataframe

    def convert_column_types(
        self,
        dataframe,
    ):
        """
        Converte colunas específicas do DataFrame para o tipo de dado adequado.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os tipos de dados das colunas convertidos.
        """
        date_columns = {
            "content_metrics": ["Date"],
            "content_posts": ["Date", "Campaign Start Date", "Campaign End Date"],
            "followers_new": ["Date"],
            "visitors_metrics": ["Date"],
        }

        columns_to_convert = date_columns.get(dataframe["dataframe_name"], [])
        columns_to_convert.append("Extraction Range")

        for column in columns_to_convert:
            dataframe["df"] = dataframe["df"].with_columns(
                pl.col(column).str.to_date("%m/%d/%Y")
            )

        return dataframe

    def clean_content_metrics_data(self, dataframe):
        """
        Limpa e processa os dados de conteúdo metricas.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os dados de métricas de conteúdo limpos.
        """
        df = dataframe["df"]

        column_type = {
            "Date": pl.String,  # temp
            "Impressions (organic)": pl.Int32,
            "Impressions (sponsored)": pl.Int32,
            "Impressions (total)": pl.Int32,
            "Unique impressions (organic)": pl.Int32,
            "Clicks (organic)": pl.Int32,
            "Clicks (sponsored)": pl.Int32,
            "Clicks (total)": pl.Int32,
            "Reactions (organic)": pl.Int32,
            "Reactions (sponsored)": pl.Int32,
            "Reactions (total)": pl.Int32,
            "Comments (organic)": pl.Int32,
            "Comments (sponsored)": pl.Int32,
            "Comments (total)": pl.Int32,
            "Shares (organic)": pl.Int32,
            "Shares (sponsored)": pl.Int32,
            "Shares (total)": pl.Int32,
            "Engagement rate (organic)": pl.Float64,
            "Engagement rate (sponsored)": pl.Float64,
            "Engagement rate (total)": pl.Float64,
        }
        df = df.cast(column_type)

        df = df.with_columns(
            pl.when(pl.col("Reactions (total)") >= 0)
            .then(pl.col("Reactions (total)"))
            .otherwise(pl.lit(0))
            .alias("Reactions (positive)"),
            pl.when(pl.col("Comments (total)") >= 0)
            .then(pl.col("Comments (total)"))
            .otherwise(pl.lit(0))
            .alias("Comments (positive)"),
            pl.when(pl.col("Shares (total)") >= 0)
            .then(pl.col("Shares (total)"))
            .otherwise(pl.lit(0))
            .alias("Shares (positive)"),
            pl.when(pl.col("Clicks (total)") >= 0)
            .then(pl.col("Clicks (total)"))
            .otherwise(pl.lit(0))
            .alias("Clicks (positive)"),
        )

        df = df.with_columns(
            (pl.col("Reactions (positive)"))
            .rolling_mean(window_size=3)
            .alias("Reactions (moving average)"),
            (pl.col("Comments (positive)"))
            .rolling_mean(window_size=3)
            .alias("Comments (moving average)"),
            (pl.col("Shares (positive)"))
            .rolling_mean(window_size=3)
            .alias("Shares (moving average)"),
            (pl.col("Clicks (positive)"))
            .rolling_mean(window_size=3)
            .alias("Clicks (moving average)"),
        )

        df = df.with_columns(
            pl.when(pl.col("Reactions (total)") >= 0)
            .then(pl.col("Reactions (total)"))
            .otherwise("Reactions (moving average)")
            .alias("Reactions (final)"),
            pl.when(pl.col("Comments (total)") >= 0)
            .then(pl.col("Comments (total)"))
            .otherwise("Comments (moving average)")
            .alias("Comments (final)"),
            pl.when(pl.col("Shares (total)") >= 0)
            .then(pl.col("Shares (total)"))
            .otherwise("Shares (moving average)")
            .alias("Shares (final)"),
            pl.when(pl.col("Clicks (total)") >= 0)
            .then(pl.col("Clicks (total)"))
            .otherwise("Clicks (moving average)")
            .alias("Clicks (final)"),
        )

        engagement_sum = (
            pl.col("Reactions (final)")
            + pl.col("Comments (final)")
            + pl.col("Clicks (final)")
            + pl.col("Shares (final)")
        )

        df = df.with_columns(
            (engagement_sum / pl.col("Impressions (total)")).alias(
                "Engagement rate (calculed)"
            )
        )

        df_final = df.select(
            [
                "Date",
                "Impressions (total)",
                "Reactions (final)",
                "Comments (final)",
                "Clicks (final)",
                "Shares (final)",
                "Engagement rate (calculed)",
                "Extraction Range",
            ]
        )
        df_final.columns = [
            "Date",
            "Impressions (total)",
            "Reactions (total)",
            "Comments (total)",
            "Clicks (total)",
            "Shares (total)",
            "Engagement Rate (total)",
            "Extraction Range",
        ]

        dataframe["df"] = df_final

        return dataframe

    def transform_data(self, data):
        """
        Aplica uma série de transformações aos dados extraídos.

        Parâmetros:
        data (list): Lista de dicionários contendo os dados extraídos.

        Retorno:
        list: Lista de dicionários contendo os dados transformados.
        """
        for dataframe in data:
            dataframe = self.translate_cols(dataframe)
            dataframe = self.add_final_date(dataframe)
            dataframe = self.convert_column_types(dataframe)
            if dataframe["dataframe_name"] == "content_metrics":
                dataframe = self.clean_content_metrics_data(dataframe)

        return data

    def load_to_clean(self, data):
        """
        Carrega os dados transformados no diretório de dados limpos.

        Parâmetros:
        data (list): Lista de dicionários contendo os dados transformados.

        Retorno:
        int: Retorna 1 se a carga for bem-sucedida.
        """
        for dataframe in data:
            dir_export = dataframe["dir"].replace("raw", "clean")
            if not os.path.exists(dir_export):
                os.makedirs(dir_export)

            export_filename = (
                dataframe["dataframe_name"]
                + "_"
                + dataframe["extraction_period"].split("-")[-1]
                + ".csv"
            )

            dataframe["df"].write_csv(
                os.path.join(dir_export, export_filename),
                quote_style="always",
                # index=False,
                # quoting=csv.QUOTE_ALL,
            )

        return 1

    def concatenate_monthly_dataframes(self, data):
        """
        Agrupa e concatena os DataFrames extraídos por mês.

        Parâmetros:
        data (list): Lista de dicionários contendo os dados extraídos.

        Retorno:
        dict: Dicionário com os DataFrames concatenados, categoria e diretório de saída.
        """
        grouped_data_month = {}

        for dataframe in data:
            year_month = "_".join(dataframe["extraction_period"].split("-")[:2])
            tag_month = f"{year_month}_{dataframe['dataframe_name']}"

            if tag_month not in grouped_data_month:
                grouped_data_month[tag_month] = {
                    "category": dataframe["dataframe_name"],
                    "export_dir": dataframe["dir"].replace("raw", "clean"),
                    "dfs": [],
                }

            grouped_data_month[tag_month]["dfs"].append(dataframe["df"])

        for tag_month, grouped_data in grouped_data_month.items():
            grouped_data_month[tag_month]["concatenated_df"] = pl.concat(
                grouped_data["dfs"]
            )

        return grouped_data_month

    def export_dataframes(self, data, file_prefix):
        """
        Exporta dataframes concatenados para um arquivo CSV.

        Parâmetros:
        data (dict): Dicionário com os DataFrames concatenados.
        file_prefix (str): Tipo de exportação (e.g., 'month', 'clean').

        Retorno:
        int: Retorna 1 se a exportação for bem-sucedida.
        """
        for key, dataframe in data.items():
            export_dir = dataframe["export_dir"]
            export_filename = f"{file_prefix}_{dataframe['category']}.csv"

            if os.path.exists(export_dir) == False:
                os.makedirs(export_dir)

            full_path = os.path.join(export_dir, export_filename)
            # dataframe["concatenated_df"].to_csv(
            #     full_path, index=False, quoting=csv.QUOTE_ALL
            # )
            dataframe["concatenated_df"].write_csv(full_path, quote_style="always")
        return 1

    def concatenate_category_dataframes(self, data):
        """
        Concatena todos os arquivos concatenados mensalmente em arquivos únicos por categoria.

        Parâmetros:
        clean_data (dict): Dicionário de listas de arquivos mensais limpos a serem concatenados.

        Retorno:
        int: Retorna 1 se a concatenação for bem-sucedida.
        """
        grouped_data_category = {}

        for key, dataframe in data.items():
            if dataframe["category"] not in grouped_data_category:
                grouped_data_category[dataframe["category"]] = {
                    "category": dataframe["category"],
                    "export_dir": re.sub(
                        r"(clean)[\\/].*",
                        r"\1/concatenated_dataframes",
                        dataframe["export_dir"],
                    ),
                    "dfs": [],
                }

            grouped_data_category[dataframe["category"]]["dfs"].append(
                dataframe["concatenated_df"]
            )

        for category, grouped_data in grouped_data_category.items():
            grouped_data_category[category]["concatenated_df"] = pl.concat(
                grouped_data["dfs"]
            )

        return grouped_data_category


def main():

    raw_directory = "data/linkedin/raw"
    clean_directory = "data/linkedin/clean"

    etl = EtlLinkedinPolars(raw_directory, clean_directory)
    data = etl.extract_data()
    data = etl.transform_data(data)
    etl.load_to_clean(data)

    concatenated_monthly_dataframes = etl.concatenate_monthly_dataframes(data)
    etl.export_dataframes(concatenated_monthly_dataframes, file_prefix="month")

    concatenated_category_dataframes = etl.concatenate_category_dataframes(
        concatenated_monthly_dataframes
    )
    etl.export_dataframes(
        concatenated_category_dataframes, file_prefix="all_extractions"
    )


if __name__ == "__main__":
    # debug
    # delete clean_dir
    import shutil

    shutil.rmtree("data/linkedin/clean")

    main()
