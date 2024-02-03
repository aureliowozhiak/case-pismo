import os
import json
import pandas as pd
import time

class Pipeline:
    def __init__(self, environment = "dev"):
        self.environment = environment
        self.folder_path = "/app/events/"
        os.makedirs(f"{self.folder_path}parquet/", exist_ok=True)

    def get_events(self, file_path: str):
        with open(file_path, "r") as json_file:
            json_data = json_file.read()
        
        return pd.json_normalize(json.loads(json_data))

    def save_parquet(self, df: pd.DataFrame, file_name: str):
        domain = df["domain"].values[0]
        event_type = df["event_type"].values[0]
        os.makedirs(f"{self.folder_path}parquet/{domain}", exist_ok=True)
        os.makedirs(f"{self.folder_path}parquet/{domain}/{event_type}", exist_ok=True)
        df.to_parquet(f"{self.folder_path}parquet/{domain}/{event_type}/{file_name}.parquet", engine="pyarrow")

    def process_events(self):
        match self.environment:
            case "dev":
                for file in os.listdir(f"{self.folder_path}json/"):
                    if file.endswith(".json"):
                        file_path = f"{self.folder_path}json/{file}"

                        df = self.get_events(file_path)
                        self.save_parquet(df, file.split(".")[0])
                        
            case _:
                raise ValueError("Invalid environment")