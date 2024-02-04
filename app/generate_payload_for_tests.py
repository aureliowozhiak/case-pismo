from faker import Faker
from datetime import datetime

class PayloadGenerator:
    """
    A class that generates payloads for testing purposes.
    """

    def __init__(self):
        """
        Initializes a new instance of the PayloadGenerator class.
        """
        self.fake = Faker()
        self.elements_status = ("ACTIVE", "INACTIVE", "SUSPENDED", "UNSUSPENDED")
        self.elements_event_type = ("status-change", "status-update", "status-activate", "status-deactivate", "status-suspend", "status-unsuspend")
        self.domain = ("account", "transaction")

    def generate_payload(self):
        """
        Generates a payload with random data.

        Returns:
            tuple: A tuple containing the event ID, event timestamp, and the payload.
        """
        old_status = self.fake.random_element(elements=self.elements_status)
        self.new_status = self.fake.random_element(elements=tuple([status for status in self.elements_status if status != old_status]))
        
        event_id = self.fake.uuid4()
        event_timestamp = self.fake.date_time_this_decade().strftime("%Y_%m_%d")

        self.payload = {
            "event_id": event_id,
            "timestamp": event_timestamp,
            "domain": self.fake.random_element(self.domain),
            "event_type": self.fake.random_element(elements=self.elements_event_type),
            "data": {
                "id": self.fake.uuid4(),
                "old_status": old_status,
                "new_status": old_status,
                "reason": self.fake.sentence()
            }
        }

        return event_id, event_timestamp, self.payload

    def duplicate_payload(self, event_id, event_timestamp):
        """
        Duplicates a payload by updating the timestamp and new status.

        Args:
            event_id (str): The event ID of the payload.
            event_timestamp (str): The event timestamp of the payload.

        Returns:
            tuple: A tuple containing the updated event ID, updated timestamp, and the duplicated payload.
        """
        self.payload["timestamp"] = self.fake.date_time_this_decade().strftime("%Y_%m_%d")
        self.payload["data"]["new_status"] = self.new_status
        return event_id, self.payload["timestamp"], self.payload

    