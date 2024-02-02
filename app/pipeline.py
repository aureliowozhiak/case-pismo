from pyspark.sql import SparkSession


class Pipeline:
    def __init__(self, environment = "dev"):
        self.environment = environment

    def get_events(self):
        match self.environment:
            case "dev":
                spark = SparkSession.builder \
                    .appName("JSON to Parquet Conversion") \
                    .getOrCreate()

                json_path = "/app/events/payload_2024_02_02_12_38_11.json"

                df = spark.read.json(json_path)

                parquet_path = "/app/events/event.parquet"

                df.write.format("parquet").save(parquet_path)

            case _:
                raise ValueError("Invalid environment")                