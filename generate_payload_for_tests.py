from faker import Faker

class PayloadGenerator:
    def __init__(self):
        self.fake = Faker()
        self.elements_status = ("ACTIVE", "INACTIVE", "SUSPENDED", "UNSUSPENDED")
        self.elements_event_type = ("status-change", "status-update", "status-activate", "status-deactivate", "status-suspend", "status-unsuspend")
        self.domain = ("account", "transaction")

    def generate_payload(self):
        old_status = self.fake.random_element(elements=self.elements_status)
        new_status = self.fake.random_element(elements=tuple([status for status in self.elements_status if status != old_status]))

        payload = {
            "event_id": self.fake.uuid4(),
            "timestamp": self.fake.date_time_this_decade().isoformat(),
            "domain": self.fake.random_element(self.domain),
            "event_type": self.fake.random_element(elements=self.elements_event_type),
            "data": {
                "id": self.fake.uuid4(),
                "old_status": old_status,
                "new_status": new_status,
                "reason": self.fake.sentence()
            }
        }

        return payload

