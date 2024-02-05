import pytest
import os

from pipeline import Pipeline

class TestPipeline:
    @pytest.fixture
    def setup_test_environment(self):
        os.makedirs("/app/events", exist_ok=True)
        os.makedirs("/app/events/test", exist_ok=True)
        os.makedirs("/app/events/test/raw_json", exist_ok=True)

        with open("/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f.json", "w") as file:
            file.write(f"""{{
                "event_id": "0acae195-47c0-4a48-b008-f9aed5e9551f",
                "timestamp": "2020_01_03",
                "domain": "transaction",
                "event_type": "status-change",
                "data": {{
                    "id": "eb29b2a3-881e-48f5-84a6-d86a7b289474",
                    "old_status": "INACTIVE",
                    "new_status": "INACTIVE",
                    "reason": "Argue major produce public nor few step drop."
                }}
            }}""")

    def test_get_events_from_json_file(self, setup_test_environment):
        pipeline = Pipeline()
        file_path = "/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f.json"
        df = pipeline.get_events(file_path)

        assert df.shape == (1, 8)
        assert df["event_id"].values[0] == "0acae195-47c0-4a48-b008-f9aed5e9551f"
        assert df["timestamp"].values[0] == "2020_01_03"
        assert df["domain"].values[0] == "transaction"
        assert df["event_type"].values[0] == "status-change"
        assert df["data.id"].values[0] == "eb29b2a3-881e-48f5-84a6-d86a7b289474"
        assert df["data.old_status"].values[0] == "INACTIVE"
        assert df["data.new_status"].values[0] == "INACTIVE"
        assert df["data.reason"].values[0] == "Argue major produce public nor few step drop."

    def test_save_staging_parquet_with_empty_folder(self, setup_test_environment):
        pipeline = Pipeline()
        df = pipeline.get_events("/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f.json")
        file_name = "2020_01_03"
        pipeline.save_staging_parquet(df, file_name)

        assert os.path.exists("/app/events/staging_parquet/transaction/status-change/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.parquet")
