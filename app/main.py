from generate_payload_for_tests import PayloadGenerator
from pipeline import Pipeline
from datetime import datetime
import pandas as pd
import json
import time
import os

payload_generator = PayloadGenerator()

for i in range(0):
    payload = payload_generator.generate_payload()
    current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    with open(f"/app/events/json/payload_{current_time}.json", "w") as file:
        file.write(str(json.dumps(payload, indent=4)))
    time.sleep(1)

pipeline = Pipeline("dev")
pipeline.get_events()

