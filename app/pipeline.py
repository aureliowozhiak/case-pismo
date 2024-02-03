import os
import json
import pandas as pd
import time



class Pipeline:
    def __init__(self, environment = "dev"):
        self.environment = environment
        os.makedirs(f"/app/events/parquet/", exist_ok=True)


    def get_events(self):
        match self.environment:
            case "dev":
                
                            
                for file in os.listdir("/app/events/json/"):
                    if file.endswith(".json"):
                        with open(f"/app/events/json/{file}", "r") as json_file:
                            json_data = json_file.read()
                        

                        df = pd.json_normalize(json.loads(json_data))
                        domain = df["domain"].values[0]
                        event_type = df["event_type"].values[0]
                                        
                        os.makedirs(f"/app/events/parquet/{domain}", exist_ok=True)
                        os.makedirs(f"/app/events/parquet/{domain}/{event_type}", exist_ok=True)

                        df.to_parquet(f"/app/events/parquet/{domain}/{event_type}/{file.split(".")[0]}.parquet")

            case _:
                raise ValueError("Invalid environment")                
