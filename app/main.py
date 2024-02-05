from generate_payload_for_tests import PayloadGenerator
from pipeline import Pipeline
from datetime import datetime
import pandas as pd
import json
import time
import os
import argparse

class EventProcessor:
    def __init__(self):
        """
        Initializes the EventProcessor class.
        """
        self.payload_generator = PayloadGenerator()

    def generate_events(self, number_of_events):
        """
        Generates events with payloads and saves them as JSON files.

        Args:
            number_of_events (int): Number of events to generate.

        Returns:
            None
        """
        # Create the JSON folder if it doesn't exist
        os.makedirs("/app/events/raw_json/", exist_ok=True)

        for i in range(number_of_events):
            # Generate payload for an event
            event_id, event_timestamp, payload = self.payload_generator.generate_payload()
            os.makedirs(f"/app/events/raw_json/{event_id}", exist_ok=True)

            # Save the payload as a JSON file
            with open(f"/app/events/raw_json/{event_id}/{event_timestamp}.json", "w") as file:
                file.write(str(json.dumps(payload, indent=4)))

            # Duplicate the payload for another event
            event_id, event_timestamp, payload = self.payload_generator.duplicate_payload(event_id, event_timestamp)

            # Save the duplicated payload as a JSON file
            with open(f"/app/events/raw_json/{event_id}/{event_timestamp}.json", "w") as file:
                file.write(str(json.dumps(payload, indent=4)))

    def process(self, environment):
        """
        Processes the events and saves them as Parquet files.

        Returns:
            None
        """
        pipeline = Pipeline(environment)
        pipeline.process()



if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--arg1', type=int, help='Number of events to generate', default=0, required=False, dest='arg1')
    args = parser.parse_args()
    number_of_events = args.arg1

    # Create an instance of EventProcessor
    event_processor = EventProcessor()

    # Generate events
    event_processor.generate_events(number_of_events)

    # Process events
    event_processor.process("local")
