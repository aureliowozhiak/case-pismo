from generate_payload_for_tests import PayloadGenerator
from datetime import datetime
import json
import time

payload_generator = PayloadGenerator()

for i in range(10):
    payload = payload_generator.generate_payload()
    current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    with open(f"events/payload_{current_time}.json", "w") as file:
        file.write(str(json.dumps(payload, indent=4)))
    time.sleep(1)