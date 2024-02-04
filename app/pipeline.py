import os
import json
import pandas as pd
import time

class Pipeline:
    def __init__(self):
        """
        Initializes a Pipeline object.

        The folder_path attribute is set to "/app/events/" by default.
        """
        self.folder_path = "/app/events/"

    def get_events(self, file_path: str):
        """
        Reads a JSON file and returns its contents as a normalized pandas DataFrame.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            pd.DataFrame: The normalized DataFrame containing the JSON data.
        """
        with open(file_path, "r") as json_file:
            json_data = json_file.read()
        
        return pd.json_normalize(json.loads(json_data))

    def save_parquet(self, df: pd.DataFrame, file_name: str):
        """
        Saves a pandas DataFrame as a Parquet file.

        The DataFrame is saved in a directory structure based on the domain, event type, and event ID.

        Args:
            df (pd.DataFrame): The DataFrame to be saved.
            file_name (str): The name of the Parquet file.
        """
        domain = df["domain"].values[0]
        event_type = df["event_type"].values[0]
        event_id = df["event_id"].values[0]

        full_path = f"{self.folder_path}parquet/{domain}/{event_type}/{event_id}".split("/")

        path = ""
        for folder in full_path:
            path += f"{folder}/"
            os.makedirs(path, exist_ok=True)

        df.to_parquet(f"{self.folder_path}parquet/{domain}/{event_type}/{event_id}/{file_name}.parquet", engine="pyarrow")

    def process_event(self, folder: str):
        """
        Processes all JSON files in a specific folder.

        For each JSON file, it reads the contents, converts them to a DataFrame, and saves it as a Parquet file.

        Args:
            folder (str): The name of the folder containing the JSON files.
        """
        for file in os.listdir(f"{self.folder_path}json/{folder}"):
            if file.endswith(".json"):
                df = self.get_events(f"{self.folder_path}json/{folder}/{file}")
                self.save_parquet(df, file.split(".")[0])

    def process_events(self):
        """
        Processes all JSON files in the "json" folder.

        It iterates over each subfolder in the "json" folder and calls the process_event method for each subfolder.
        """
        for folder in os.listdir(f"{self.folder_path}json/"):
            if os.path.isdir(f"{self.folder_path}json/{folder}"):
                self.process_event(folder)
                