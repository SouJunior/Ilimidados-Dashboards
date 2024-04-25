import pandas as pd
import os
import csv

import warnings

warnings.simplefilter("ignore")


class EtlLinkedin:
    def __init__(self, extraction_daterange="365d"):
        self.path_etl = os.path.join(
            os.getcwd(), "linkedin", "data", "raw", extraction_daterange
        )
        self.path_export = os.path.join(
            os.getcwd(), "linkedin", "data", "processed", extraction_daterange
        )
        self.last_date = None  # O valor é atribuido durante o loop de limpeza e utilizado em transform_data, caso a tabela não tenha a medida de tempo

    def mass_etl(self, extractions_dates: list):
        self.clear_output()  # Temporário, processar dados passando uma lista de extrações enquanto não houver validação do mentor

        for folder_date in extractions_dates:
            self.process_data(folder_date)

    def process_data(self, folder_date):
        print("Processing:", folder_date)
        extraction_files_path = os.path.join(self.path_etl, folder_date)
        print("Reading files...")
        dfs = self.read_files(extraction_files_path)
        print("Transforming data...")
        transformed_dfs = self.transform(dfs)
        print("Concatenating data...")
        concatenated_dfs = self.concatenate(transformed_dfs, self.path_export)
        print("Exporting data...")
        self.export(concatenated_dfs)
        print("Done!")

    def clear_output(self):
        if not os.path.exists(self.path_export):
            os.makedirs(self.path_export)
        else:
            for file in os.listdir(self.path_export):
                os.remove(os.path.join(self.path_export, file))

    def read_files(self, path):

        files_in_path = os.listdir(path)
        try:
            path_competitor = (
                path
                + "/"
                + [
                    file
                    for file in files_in_path
                    if "concorrente" in file or "competitor" in file
                ][0]
            )
            path_content = (
                path
                + "/"
                + [
                    file
                    for file in files_in_path
                    if "conteudo" in file or "content" in file or "conteúdo" in file
                ][0]
            )
            path_followers = (
                path
                + "/"
                + [
                    file
                    for file in files_in_path
                    if "seguidor" in file or "followers" in file
                ][0]
            )
            path_visitors = (
                path
                + "/"
                + [
                    file
                    for file in files_in_path
                    if "visitante" in file or "visitors" in file
                ][0]
            )
        except Exception as e:
            print("Erro ao encontrar arquivos de entrada. Verifique o diretório.")
            print(str(e))
            exit()

        dfs = [
            {
                "name": "content_metrics",
                "path": path_content,
                "sheet": 0,
                "skiprows": 1,
            },
            {"name": "content_posts", "path": path_content, "sheet": 1, "skiprows": 1},
            {"name": "followers_new", "path": path_followers, "sheet": 0},
            {"name": "followers_location", "path": path_followers, "sheet": 1},
            {"name": "followers_function", "path": path_followers, "sheet": 2},
            {"name": "followers_experience", "path": path_followers, "sheet": 3},
            {"name": "followers_industry", "path": path_followers, "sheet": 4},
            {"name": "followers_company_size", "path": path_followers, "sheet": 5},
            {"name": "followers_company_size", "path": path_followers, "sheet": 5},
            {"name": "visitors_metrics", "path": path_visitors, "sheet": 0},
            {"name": "visitors_location", "path": path_visitors, "sheet": 1},
            {"name": "visitors_function", "path": path_visitors, "sheet": 2},
            {"name": "visitors_experience", "path": path_visitors, "sheet": 3},
            {"name": "visitors_industry", "path": path_visitors, "sheet": 4},
            {"name": "visitors_company_size", "path": path_visitors, "sheet": 5},
            {"name": "competitors", "path": path_competitor, "skiprows": 1},
        ]

        for df in dfs:
            if ".xls" in df["path"]:
                df["df"] = pd.read_excel(
                    df["path"],
                    sheet_name=df.get("sheet", 0),
                    skiprows=df.get("skiprows", 0),
                )
            else:
                df["df"] = pd.read_csv(df["path"], skiprows=df.get("skiprows", 0))

        return dfs

    def transform(self, dfs):
        for df in dfs:
            if df["name"] == "competitors":
                df["df"] = df["df"][
                    [
                        "Page",
                        "Total de seguidores",
                        "Novos seguidores",
                        "Total de engajamentos da publicação",
                        "Total de publicações",
                    ]
                ]

            df["df"] = self.transform_data(df["df"], category=df["name"])

            if df["name"] == "content_metrics":
                self.last_date = self.get_last_date(df["df"])
                print("Ultima data coletada -", self.last_date)

        return dfs

    def transform_data(self, df, category):
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
            "competitors": [
                "Page",
                "Total Followers",
                "New Followers",
                "Total Post Engagements",
                "Total Posts",
            ],
        }

        df.columns = english_cols.get(category)

        if category == "content_metrics":
            df = df[
                [
                    "Date",
                    "Impressions",
                    "Clicks",
                    "Reactions",
                    "Comments",
                    "Shares",
                    "Engagement rate",
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
                    row["Shares (moving average)"]
                    if row["Shares"] < 0
                    else row["Shares"]
                ),
                axis=1,
            )

            df["Clicks"] = df.apply(
                lambda row: (
                    row["Clicks (moving average)"]
                    if row["Clicks"] < 0
                    else row["Clicks"]
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

            df = df[
                [
                    "Date",
                    "Impressions",
                    "Clicks",
                    "Reactions",
                    "Comments",
                    "Shares",
                    "Engagement Rate",
                ]
            ]

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])

        else:
            df.insert(0, "Date", self.last_date)

        return df

    def get_last_date(self, df):
        return df["Date"].max().date()

    def concatenate(self, dfs, path_export):
        if len(os.listdir(path_export)) > 0:
            print("Clean data detected! Concatenating...")
            for df in dfs:
                df_clean = pd.read_csv(
                    path_export + "\\" + df["name"] + ".csv", parse_dates=["Date"]
                )

                if df["name"] == "content_metrics":
                    df["df"] = self.concat_dfs(
                        df_clean, df["df"], drop_duplicates="Date"
                    )
                elif df["name"] == "content_posts":
                    df["df"] = self.concat_dfs(
                        df_clean, df["df"], drop_duplicates="Post Link"
                    )
                elif df["name"] == "followers_new":
                    df["df"] = self.concat_dfs(
                        df_clean, df["df"], drop_duplicates="Date"
                    )
                elif df["name"] == "visitors_metrics":
                    df["df"] = self.concat_dfs(
                        df_clean, df["df"], drop_duplicates="Date"
                    )
                else:
                    df["df"] = self.concat_dfs(df_clean, df["df"])
        else:
            print("No data to concatenate! Continuing...")

        return dfs

    def concat_dfs(
        self,
        df1,
        df2,
        drop_duplicates=False,
    ):
        df1["Date"] = pd.to_datetime(df1["Date"])
        df2["Date"] = pd.to_datetime(df2["Date"])
        df = pd.concat([df1, df2])

        if drop_duplicates:
            df = df.drop_duplicates(subset=[drop_duplicates])

        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        df = df.sort_values(by="Date", ascending=False)

        return df

    def export(self, dfs):
        
        for df in dfs:
            filepath = self.path_export + f"/{df['name']}.csv"
            df["df"].to_csv(filepath, index=False, quoting=csv.QUOTE_ALL)



if __name__ == "__main__":
    etl = EtlLinkedin()
    etl.mass_etl(["01_23-01_24", "03_23-03_24", "04_23-04_24"])
