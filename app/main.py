from generate_payload_for_tests import PayloadGenerator
from pipeline import Pipeline
from datetime import datetime
import pandas as pd
import json
import time
import os

payload_generator = PayloadGenerator()

os.makedirs("/app/events/json/", exist_ok=True)

for i in range(1):
    event_id, event_timestamp, payload = payload_generator.generate_payload()
    os.makedirs(f"/app/events/json/{event_timestamp}", exist_ok=True)

    with open(f"/app/events/json/{event_timestamp}/{event_id}.json", "w") as file:
        file.write(str(json.dumps(payload, indent=4)))
    

    
    event_id, event_timestamp, payload = payload_generator.duplicate_payload(event_id, event_timestamp)
    os.makedirs(f"/app/events/json/{event_timestamp}", exist_ok=True)

    with open(f"/app/events/json/{event_timestamp}/{event_id}.json", "w") as file:
        file.write(str(json.dumps(payload, indent=4)))

pipeline = Pipeline("dev")
pipeline.process_events()