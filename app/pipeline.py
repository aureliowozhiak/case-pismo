import os
import json
import pandas as pd
import time
from datetime import datetime

class Pipeline:
    def __init__(self, environment="local"):
        """
        Initializes a Pipeline object.

        The folder_path attribute is set to "/app/events/" by default.
        """
        self.folder_path = "/app/events/"
        self.environment = environment

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

    def save_staging_parquet(self, df: pd.DataFrame, file_name: str):
        """
        Saves a pandas DataFrame as a Parquet file.

        The DataFrame is saved in a directory structure based on the domain, event type, and event ID.
        Save the DataFrame as a Parquet file in the following format: /app/events/staging_parquet/{domain}/{event_type}/{event_id}/{file_name}.parquet
        Just save the DataFrame as a Parquet file if the file_name is greater than or equal to the date in the folder.

        Args:
            df (pd.DataFrame): The DataFrame to be saved.
            file_name (str): The name of the Parquet file.
        """
        domain = df["domain"].values[0]
        event_type = df["event_type"].values[0]
        event_id = df["event_id"].values[0]

        full_path = f"{self.folder_path}staging_parquet/{domain}/{event_type}/{event_id}".split("/")

        path = ""
        for folder in full_path:
            path += f"{folder}/"
            os.makedirs(path, exist_ok=True)

        files_in_folder = os.listdir(path)

        # If the folder is empty, save the DataFrame as a Parquet file
        if len(files_in_folder) == 0:
            df.to_parquet(f"{self.folder_path}staging_parquet/{domain}/{event_type}/{event_id}/{file_name}.parquet", engine="pyarrow")
        
        # If the folder is not empty, compare the file_name with the date in the folder
        else:
            date_in_folder = datetime.strptime(files_in_folder[0].split(".")[0], "%Y_%m_%d")
            date_in_df = datetime.strptime(file_name, "%Y_%m_%d")

            # If the file_name is greater than or equal to the date in the folder, save the DataFrame as a Parquet file
            if date_in_df >= date_in_folder:
                # Save the DataFrame as a Parquet file
                df.to_parquet(f"{self.folder_path}staging_parquet/{domain}/{event_type}/{event_id}/{file_name}.parquet", engine="pyarrow")
                # Remove the old Parquet file
                os.remove(f"{self.folder_path}staging_parquet/{domain}/{event_type}/{event_id}/{files_in_folder[0]}")

    def process_event(self, folder: str):
        """
        Processes all JSON files in a specific folder.

        For each JSON file, it reads the contents, converts them to a DataFrame, and saves it as a Parquet file.

        Args:
            folder (str): The name of the folder containing the JSON files.
        """
        for file in os.listdir(f"{self.folder_path}raw_json/{folder}"):
            if file.endswith(".json"):
                df = self.get_events(f"{self.folder_path}raw_json/{folder}/{file}")
                self.save_staging_parquet(df, file.split(".")[0])


    def process_events(self):
        """
        Processes all JSON files in the "json" folder.

        It iterates over each subfolder in the "json" folder and calls the process_event method for each subfolder.
        """
        for folder in os.listdir(f"{self.folder_path}raw_json/"):
            if os.path.isdir(f"{self.folder_path}raw_json/{folder}"):
                self.process_event(folder)

    def move_and_aggregate(self):
        """
        Moves the Parquet files from the "staging_parquet" folder to the "aggregated_parquet" folder.
        
        It also aggregates the Parquet files by date and event type.
        
        """
        for root, dirs, files in os.walk(f"{self.folder_path}staging_parquet/"):
            for file in files:
                if file.endswith(".parquet"):
                    domain = root.split("/")[4]
                    event_type = root.split("/")[5]
                    event_id = root.split("/")[6]
                    date = file.split(".")[0]

                    domain_event_type = f"{domain}_{event_type}"

                    date = date.replace("_", "/")

                    full_path = f"{self.folder_path}aggregated_parquet/{date}/{domain_event_type}".split("/")

                    path = ""
                    for folder in full_path:
                        path += f"{folder}/"
                        os.makedirs(path, exist_ok=True)

                    df = pd.read_parquet(f"{root}/{file}")
                    df.to_parquet(f"{self.folder_path}aggregated_parquet/{date}/{domain_event_type}/{event_id}.parquet", engine="pyarrow")
                    os.remove(f"{root}/{file}")


    def process(self):
        match self.environment:
            case "local":
                self.process_events()
                self.move_and_aggregate()
            case "production":
                pass
            case _:
                print("Invalid environment")
