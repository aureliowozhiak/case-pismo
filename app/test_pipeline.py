import pytest
import os

from pipeline import Pipeline

class TestPipeline:
    """
    This class contains unit tests for the Pipeline class.
    """

    @pytest.fixture
    def setup_test_environment(self):
        """
        Set up the test environment by creating necessary directories and files.
        """

        os.makedirs("/app/events", exist_ok=True)
        os.makedirs("/app/events/test", exist_ok=True)
        os.makedirs("/app/events/test/raw_json", exist_ok=True)
        os.makedirs("/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f", exist_ok=True)

        with open("/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.json", "w") as file:
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
        """
        Test the get_events method of the Pipeline class by reading events from a JSON file.
        """

        pipeline = Pipeline(folder_path="/app/events/test/")
        file_path = "/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.json"
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


    def test_save_staging_parquet(self, setup_test_environment):
        """
        Test the save_staging_parquet method of the Pipeline class by saving DataFrame as a Parquet file.
        """

        pipeline = Pipeline(folder_path="/app/events/test/")
        file_path = "/app/events/test/raw_json/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.json"
        df = pipeline.get_events(file_path)
        file_name = "2020_01_03"
        pipeline.save_staging_parquet(df, file_name)

        assert os.path.exists("/app/events/test/staging_parquet/transaction/status-change/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.parquet")
        
    def test_process_event(self, setup_test_environment):
        """
        Test the process_event method of the Pipeline class by processing a single event folder.
        """

        os.remove("/app/events/test/staging_parquet/transaction/status-change/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.parquet")
        pipeline = Pipeline(folder_path="/app/events/test/")
        folder = "0acae195-47c0-4a48-b008-f9aed5e9551f"
        pipeline.process_event(folder)

        assert os.path.exists("/app/events/test/staging_parquet/transaction/status-change/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.parquet")

    def test_process_events(self, setup_test_environment):
        """
        Test the process_events method of the Pipeline class by processing all event folders.
        """

        os.remove("/app/events/test/staging_parquet/transaction/status-change/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.parquet")
        pipeline = Pipeline(folder_path="/app/events/test/")
        pipeline.process_events()

        assert os.path.exists("/app/events/test/staging_parquet/transaction/status-change/0acae195-47c0-4a48-b008-f9aed5e9551f/2020_01_03.parquet")

    def test_copy_and_aggregate_staging_parquet(self, setup_test_environment):
        """
        Test the copy_and_aggregate method of the Pipeline class by copying and aggregating staging Parquet files.
        """

        pipeline = Pipeline(folder_path="/app/events/test/")
        pipeline.copy_and_aggregate()

        assert os.path.exists("/app/events/test/aggregated_parquet/2020/01/03/transaction_status-change/0acae195-47c0-4a48-b008-f9aed5e9551f.parquet")
      