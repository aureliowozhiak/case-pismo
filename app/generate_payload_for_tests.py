from faker import Faker
from datetime import datetime

class PayloadGenerator:
    def __init__(self):
        self.fake = Faker()
        self.elements_status = ("ACTIVE", "INACTIVE", "SUSPENDED", "UNSUSPENDED")
        self.elements_event_type = ("status-change", "status-update", "status-activate", "status-deactivate", "status-suspend", "status-unsuspend")
        self.domain = ("account", "transaction")

    def generate_payload(self):
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
        self.payload["timestamp"] = self.fake.date_time_this_decade().strftime("%Y_%m_%d")
        self.payload["data"]["new_status"] = self.new_status
        return event_id, self.payload["timestamp"], self.payload

    