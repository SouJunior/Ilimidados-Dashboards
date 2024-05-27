import pandas as pd
import os
import csv

import warnings

warnings.simplefilter("ignore")


class EtlLinkedin:
    def __init__(self, dir_raw, dir_clean):
        self.dir_raw = dir_raw
        self.dir_clean = dir_clean

    def detect_category(self, file):
        if "competitor" in file:
            return "competitor"
        elif "content" in file:
            return "content"
        elif "followers" in file:
            return "followers"
        elif "visitors" in file:
            return "visitors"
        return 0

    def detect_raw_files(self, dir_raw):
        extraction_files = []
        for category in os.listdir(dir_raw):
            path_category = os.path.join(dir_raw, category)

            for year in os.listdir(path_category):
                path_year = os.path.join(path_category, year)

                for month in os.listdir(path_year):
                    path_month = os.path.join(path_year, month)

                    files_in_month = os.listdir(path_month)
                    if not files_in_month:
                        continue

                    for i, file in enumerate(files_in_month):
                        file_path = os.path.join(path_month, file)
                        df_category = self.detect_category(file)
                        extraction_files.append(
                            {
                                "category": df_category,
                                "file_path": file_path,
                                "dir": path_month,
                                "extraction_period": f"{year}-{month}-{i+1}",
                            }
                        )
        return extraction_files

    def read_file(self, file):
        # file {category, path, extraction_period}
        category_keys = {
            "competitor": [{"sheet_name": "competitor", "sheet_pos": 0, "skiprows": 1}],
            "content": [
                {"sheet_name": "content_metrics", "sheet_pos": 0, "skiprows": 1},
                {"sheet_name": "content_posts", "sheet_pos": 1, "skiprows": 1},
            ],
            "followers": [
                {"sheet_name": "followers_new", "sheet_pos": 0, "skiprows": 0},
                {"sheet_name": "followers_location", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "followers_function", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "followers_experience", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "followers_industry", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "followers_company_size", "sheet_pos": 5, "skiprows": 0},
            ],
            "visitors": [
                {"sheet_name": "visitors_metrics", "sheet_pos": 0, "skiprows": 0},
                {"sheet_name": "visitors_location", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "visitors_function", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "visitors_experience", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "visitors_industry", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "visitors_company_size", "sheet_pos": 5, "skiprows": 0},
            ],
        }

        dataframe_sheets = category_keys[file["category"]]

        dataframes = []
        for sheet in dataframe_sheets:

            df = pd.read_excel(
                file["file_path"],
                sheet_name=sheet["sheet_pos"],
                skiprows=sheet["skiprows"],
            )

            dataframes.append(
                {
                    "dataframe_name": sheet["sheet_name"],
                    "dir": file["dir"],
                    "extraction_period": file["extraction_period"],
                    "df": df,
                }
            )

        return dataframes

    def read_data(self):

        files = self.detect_raw_files(self.dir_raw)

        data = [obj for file in files for obj in self.read_file(file)]
        return data

    def translate_cols(self, dataframe):
        english_cols = {
            "content_metrics": [
                "Date",
                "Impressions (organic)",
                "Impressions (sponsored)",
                "Impressions",
                "Unique impressions (organic)",
                "Clicks (organic)",
                "Clicks (sponsored)",
                "Clicks",
                "Reactions (organic)",
                "Reactions (sponsored)",
                "Reactions",
                "Comments (organic)",
                "Comments (sponsored)",
                "Comments",
                "Shares (organic)",
                "Shares (sponsored)",
                "Shares",
                "Engagement rate (organic)",
                "Engagement rate (sponsored)",
                "Engagement rate",
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

        dataframe["df"].columns = english_cols.get(dataframe["dataframe_name"])
        return dataframe

    def insert_final_date(self, dataframe):

        map_months_period = {
            "2023-Jan-1": "2023-01-15",
            "2023-Jan-2": "2023-01-31",
            "2023-Fev-1": "2023-02-15",
            "2023-Fev-2": "2023-02-28",
            "2023-Mar-1": "2023-03-15",
            "2023-Mar-2": "2023-03-31",
            "2023-Abr-1": "2023-04-15",
            "2023-Abr-2": "2023-04-30",
            "2023-Mai-1": "2023-05-15",
            "2023-Mai-2": "2023-05-31",
            "2023-Jun-1": "2023-06-15",
            "2023-Jun-2": "2023-06-30",
            "2023-Jul-1": "2023-07-15",
            "2023-Jul-2": "2023-07-31",
            "2023-Ago-1": "2023-08-15",
            "2023-Ago-2": "2023-08-31",
            "2023-Set-1": "2023-09-15",
            "2023-Set-2": "2023-09-30",
            "2023-Out-1": "2023-10-15",
            "2023-Out-2": "2023-10-31",
            "2023-Nov-1": "2023-11-15",
            "2023-Nov-2": "2023-11-30",
            "2023-Dez-1": "2023-12-15",
            "2023-Dez-2": "2023-12-31",
            "2024-Jan-1": "2024-01-15",
            "2024-Jan-2": "2024-01-31",
            "2024-Fev-1": "2024-02-15",
            "2024-Fev-2": "2024-02-29",
            "2024-Mar-1": "2024-03-15",
            "2024-Mar-2": "2024-03-31",
            "2024-Abr-1": "2024-04-15",
            "2024-Abr-2": "2024-04-30",
            "2024-Mai-1": "2024-05-15",
            "2024-Mai-2": "2024-05-31",
            "2024-Jun-1": "2024-06-15",
            "2024-Jun-2": "2024-06-30",
            "2024-Jul-1": "2024-07-15",
            "2024-Jul-2": "2024-07-31",
            "2024-Ago-1": "2024-08-15",
            "2024-Ago-2": "2024-08-31",
            "2024-Set-1": "2024-09-15",
            "2024-Set-2": "2024-09-30",
            "2024-Out-1": "2024-10-15",
            "2024-Out-2": "2024-10-31",
            "2024-Nov-1": "2024-11-15",
            "2024-Nov-2": "2024-11-30",
            "2024-Dez-1": "2024-12-15",
            "2024-Dez-2": "2024-12-31",
        }

        final_date = map_months_period[dataframe["extraction_period"]]
        dataframe["df"]["Extraction Range"] = final_date
        return dataframe

    def convert_data_types(self, dataframe):
        date_columns = {
            "content_metrics": ["Date"],
            "content_posts": ["Date", "Campaign Start Date", "Campaign End Date"],
            "followers_new": ["Date"],
            "visitors_metrics": ["Date"],
        }

        columns_to_convert = date_columns.get(dataframe["dataframe_name"], [])
        columns_to_convert.append("Extraction Range")

        for column in columns_to_convert:
            dataframe["df"][column] = pd.to_datetime(dataframe["df"][column])

        return dataframe

    def clean_content_metrics(self, dataframe):
        df = dataframe["df"][
            [
                "Date",
                "Impressions",
                "Clicks",
                "Reactions",
                "Comments",
                "Shares",
                "Engagement rate",
                "Extraction Range",
            ]
        ]

        df["Reactions (positive)"] = df["Reactions"][df["Reactions"] >= 0]
        df["Comments (positive)"] = df["Comments"][df["Comments"] >= 0]
        df["Shares (positive)"] = df["Shares"][df["Shares"] >= 0]
        df["Clicks (positive)"] = df["Clicks"][df["Clicks"] >= 0]

        df["Reactions (positive)"] = df["Reactions (positive)"].fillna(0)
        df["Comments (positive)"] = df["Comments (positive)"].fillna(0)
        df["Shares (positive)"] = df["Shares (positive)"].fillna(0)
        df["Clicks (positive)"] = df["Clicks (positive)"].fillna(0)

        window = 3

        df["Reactions (moving average)"] = (
            df["Reactions (positive)"].rolling(window=window).mean()
        )
        df["Comments (moving average)"] = (
            df["Comments (positive)"].rolling(window=window).mean()
        )
        df["Shares (moving average)"] = (
            df["Shares (positive)"].rolling(window=window).mean()
        )
        df["Clicks (moving average)"] = (
            df["Clicks (positive)"].rolling(window=window).mean()
        )

        df["Reactions"] = df.apply(
            lambda row: (
                row["Reactions (moving average)"]
                if row["Reactions"] < 0
                else row["Reactions"]
            ),
            axis=1,
        )

        df["Comments"] = df.apply(
            lambda row: (
                row["Comments (moving average)"]
                if row["Comments"] < 0
                else row["Comments"]
            ),
            axis=1,
        )

        df["Shares"] = df.apply(
            lambda row: (
                row["Shares (moving average)"] if row["Shares"] < 0 else row["Shares"]
            ),
            axis=1,
        )

        df["Clicks"] = df.apply(
            lambda row: (
                row["Clicks (moving average)"] if row["Clicks"] < 0 else row["Clicks"]
            ),
            axis=1,
        )

        df["Engagement Rate"] = df.apply(
            lambda row: (
                row["Reactions"] + row["Comments"] + row["Clicks"] + row["Shares"]
            )
            / row["Impressions"],
            axis=1,
        )

        dataframe["df"] = df[
            [
                "Date",
                "Impressions",
                "Clicks",
                "Reactions",
                "Comments",
                "Shares",
                "Engagement Rate",
                "Extraction Range",
            ]
        ]

        return dataframe

    def transform(self, data):

        for dataframe in data:

            dataframe = self.translate_cols(dataframe)
            dataframe = self.insert_final_date(dataframe)
            dataframe = self.convert_data_types(dataframe)
            if dataframe["dataframe_name"] == "content_metrics":
                dataframe = self.clean_content_metrics(dataframe)

        return data

    def load_to_clean(self, data):
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

            dataframe["df"].to_csv(
                os.path.join(dir_export, export_filename),
                index=False,
                quoting=csv.QUOTE_ALL,
            )

        return 1

    def read_clean_months(self, dir_clean):
        files_to_concat = {
            "competitor": [],
            "content_metrics": [],
            "content_posts": [],
            "followers_new": [],
            "followers_location": [],
            "followers_function": [],
            "followers_experience": [],
            "followers_industry": [],
            "followers_company_size": [],
            "visitors_metrics": [],
            "visitors_location": [],
            "visitors_function": [],
            "visitors_experience": [],
            "visitors_industry": [],
            "visitors_company_size": [],
        }

        for category in os.listdir(dir_clean):
            path_category = os.path.join(dir_clean, category)

            for year in os.listdir(path_category):

                path_year = os.path.join(path_category, year)

                for month in os.listdir(path_year):

                    path_month = os.path.join(path_year, month)

                    files_in_month = os.listdir(path_month)

                    if not files_in_month:

                        continue

                    for file in files_in_month:
                        if file.startswith("month_"):
                            file_category = file[6:-4]
                            files_to_concat[file_category].append(
                                os.path.join(path_month, file)
                            )

        return files_to_concat

    def concat_dfs(self, files):
        dataframes = [pd.read_csv(file) for file in files]
        concatenated_df = pd.concat(dataframes, ignore_index=True)
        return concatenated_df

    def export_concatenated_df(self, df, category, output_dir, export_type):
        if os.path.exists(output_dir) == False:
            os.makedirs(output_dir)

        full_path = os.path.join(output_dir, f"{export_type}_{category}.csv")
        df.to_csv(full_path, index=False, quoting=csv.QUOTE_ALL)
        return 1

    def concat_months(self, dir_clean):
    

        category_keys = {
            "Concorrentes": ["competitor"],
            "Conteúdo": ["content_metrics", "content_posts"],
            "Seguidores": [
                "followers_new",
                "followers_location",
                "followers_function",
                "followers_experience",
                "followers_industry",
                "followers_company_size",
            ],
            "Visitantes": [
                "visitors_metrics",
                "visitors_location",
                "visitors_function",
                "visitors_experience",
                "visitors_industry",
                "visitors_company_size",
            ],
        }

        for category in os.listdir(dir_clean):

            path_category = os.path.join(dir_clean, category)

            for year in os.listdir(path_category):

                path_year = os.path.join(path_category, year)

                for month in os.listdir(path_year):

                    path_month = os.path.join(path_year, month)

                    files_in_month = os.listdir(path_month)

                    if not files_in_month:

                        continue

                    category_files = category_keys[category]
                    for category_file in category_files:
                        files_to_process = [
                            os.path.join(path_month, file)
                            for file in os.listdir(path_month)
                            if file.startswith(category_file)
                        ]
                        concatenated_df = self.concat_dfs(files_to_process)
                        self.export_concatenated_df(
                            df=concatenated_df,
                            category=category_file,
                            output_dir=path_month,
                            export_type="month",
                        )
        return 1

    def concat_all_periods(self, clean_data):
        for category, files in clean_data.items():
            concatenated_df = self.concat_dfs(files)

            split_path = files[0].split("clean")
            category_file = split_path[1].split("\\")[-1][6:-4]
            final_path = os.path.join(split_path[0], "clean", "Concatenated")

            self.export_concatenated_df(
                df=concatenated_df,
                category=category_file,
                output_dir=final_path,
                export_type="clean",
            )

        return 1


def main():
    """
    Função principal que executa as operações ETL Linkedin.
    """

    dir_raw = "data/linkedin/raw"
    dir_clean = "data/linkedin/clean"

    etl = EtlLinkedin(dir_raw, dir_clean)
    data = etl.read_data()
    data = etl.transform(data)
    etl.load_to_clean(data)

    etl.concat_months(dir_clean)

    months_data = etl.read_clean_months(dir_clean)
    etl.concat_all_periods(months_data)


if __name__ == "__main__":
    main()

