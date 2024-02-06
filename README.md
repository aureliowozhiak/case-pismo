# Case Pismo:  Transforming json event payload data into parquet

This project is a pipeline for processing JSON files and saving them as Parquet files. It provides methods for reading JSON files from a local directory, converting them to pandas DataFrames, and saving them as Parquet files. The pipeline can be run locally or in the cloud with docker.

## Summary

This is the summary of the README.md document:

1. [Getting Started](#getting-started)
2. [Technical Usage](#technical-usage)
3. [Technical Documentation of Tests in the TestPipeline Class](#technical-documentation-of-tests-in-the-testpipeline-class)

## Getting Started

To get started with this project, follow these steps:

1. Clone the repository: `git clone https://github.com/aureliowozhiak/case-pismo.git`
2. Run `docker compose up`


### Explanation of Docker Compose

The `docker compose up` command is used to start the Docker containers defined in the `docker-compose.yml` file. In this project, it starts a container named `etl` using the `python:latest` image. 

The container is configured with two volumes: one for the application code (`./app:/app:rw`). The command `bash -c "pip install -r /app/requirements.txt && rm -rf /app/events/* && python /app/main.py -n 10"` is executed inside the container. This command installs the required dependencies and runs the `main.py` script with the `-n 10` argument, which is responsible for creating 10 example payloads.

#### To test the code

If you want to test the code, you can change the `docker-compose.yml` 

*line 8* from:

```yaml
command: bash -c "pip install -r /app/requirements.txt && rm -rf /app/events/* && python /app/main.py -n 10"
```

to:

```yaml
command: bash -c "pip install -r /app/requirements.txt && rm -rf /app/events/* && pytest /app/test_pipeline.py"
```

After making this change, execute `docker-compose up` again.


⚠️ Make sure you have Docker installed and running on your machine before running the `docker compose up` command.

### What is the expected behavior after running 'docker compose up'?

The flow will generate payload data in JSON format, in `./app/events/raw_json/` folder structure.

![](/doc/raw_folder.png)

Once the example events are generated, the pipeline will process the JSON data by reading the contents, converting them into pandas DataFrames, and saving them as Parquet files. The processed data will be stored in the `./app/events/staging_parquet` directory:

![](/doc/staging_folder.png)

After this, the parquet data will be copied to the aggregated by year/month/day/type layer in the `./app/events/aggregated_parquet`:

![](/doc/aggregated_folder.png)

Note: The pipeline can be run locally or in a production environment. For more information on the technical usage and available methods, refer to the section above.


## Technical Usage

Before seeing the technical details, it is important to visualize the data pipeline flow to understand the moments of each layer (raw, staging and aggregated).

![](/doc/pipeline_flow.png)

The pipeline provides the following methods:

The `Pipeline` class is designed to process and aggregate JSON data, converting it into normalized Pandas DataFrames and saving the results as Parquet files. The class is equipped with methods to handle various stages of data processing, including retrieving events from JSON files, saving staging Parquet files based on domain, event type, and event ID, processing events in specific folders, and finally moving and aggregating Parquet files.

### Constructor

#### `__init__(self, environment="local")`
- Initializes a `Pipeline` object.
- Default `folder_path` is set to "/app/events/".
- Parameters:
  - `environment` (str): Specifies the environment as "local" (default) or "production".

### Methods

#### `get_events(self, file_path: str) -> pd.DataFrame`
- Reads a JSON file and returns its contents as a normalized Pandas DataFrame.
- Parameters:
  - `file_path` (str): The path to the JSON file.
- Returns:
  - `pd.DataFrame`: Normalized DataFrame containing the JSON data.

#### `save_staging_parquet(self, df: pd.DataFrame, file_name: str) -> None`
- Saves a Pandas DataFrame as a Parquet file in a structured directory.
- Parameters:
  - `df` (`pd.DataFrame`): The DataFrame to be saved.
  - `file_name` (str): The name of the Parquet file.

#### `process_event(self, folder: str) -> None`
- Processes all JSON files in a specific folder.
- Reads JSON files, converts their contents to DataFrames, and saves them as Parquet files.
- Parameters:
  - `folder` (str): The name of the folder containing the JSON files.

#### `process_events(self) -> None`
- Processes all JSON files in the "json" folder and calls `process_event` method for each subfolder.

#### `move_and_aggregate(self) -> None`
- Moves Parquet files from "staging_parquet" to "aggregated_parquet," aggregating them by date and event type.

#### `process(self) -> None`
- Main processing method based on the specified environment.
  - In "local" environment, it processes events and moves/aggregates Parquet files.
  - In "production" environment, it processes events and moves/aggregates Parquet file.
  - Prints an error message for an invalid environment.

### Example Usage:

```python
# Instantiate the Pipeline class
pipeline = Pipeline(environment="local")
# Process events and move/aggregate Parquet files
pipeline.process()
```

This documentation provides a comprehensive overview of the `Pipeline` class, its constructor, and methods, aiding users in understanding and effectively utilizing the functionality it offers for processing and aggregating JSON data.

## Technical Documentation of Tests in the TestPipeline Class

This part of the document describes the automated tests being executed in the `TestPipeline` class. These tests were developed to verify the proper functioning of methods in the `Pipeline` class under different scenarios.

### Context
The `Pipeline` class is responsible for processing events, reading them from JSON files, saving them in Parquet format, and performing copy and aggregation operations. The automated tests were developed using the `pytest` testing framework.

#### Test 1: `test_get_events_from_json_file`
This test verifies whether the `get_events` method of the `Pipeline` class can correctly read a JSON file of events and convert its content into a pandas DataFrame.

##### Setup
The test environment is set up by creating necessary directories and files to simulate the folder and file structure where events are stored.

##### Test
The `get_events` method is called with the path of the JSON file created during setup. The resulting data is checked to ensure it matches the expected values.

##### Asserts
- Verifies if the resulting DataFrame has the correct shape.
- Verifies if the values of columns in the DataFrame match the expected values.

#### Test 2: `test_save_staging_parquet`
This test verifies whether the `save_staging_parquet` method of the `Pipeline` class can save a DataFrame as a Parquet file in the correct location.

##### Setup
The test environment is set up in the same way as the previous test.

##### Test
The `save_staging_parquet` method is called with the DataFrame obtained during the previous test and a file name. After execution, it is checked whether the Parquet file was correctly saved in the expected location.

##### Asserts
- Verifies if the Parquet file was correctly created at the specified location.

#### Test 3: `test_process_event`
This test verifies whether the `process_event` method of the `Pipeline` class can correctly process a single event.

##### Setup
The test environment is set up in the same way as the previous tests. Additionally, the Parquet file resulting from the previous test is removed to simulate a fresh processing of the event.

##### Test
The `process_event` method is called with the name of the folder containing the event to be processed. After execution, it is checked whether the resulting Parquet file was created in the expected location.

##### Asserts
- Verifies if the Parquet file was correctly created at the specified location.

#### Test 4: `test_process_events`
This test verifies whether the `process_events` method of the `Pipeline` class can correctly process all events present in the directory.

##### Setup
The test environment is set up in the same way as the previous tests. Additionally, the Parquet file resulting from the previous test is removed to simulate fresh processing of events.

##### Test
The `process_events` method is called to process all events present in the directory. After execution, it is checked whether the resulting Parquet file was created in the expected location.

##### Asserts
- Verifies if the Parquet file was correctly created at the specified location.

#### Test 5: `test_copy_and_aggregate_staging_parquet`
This test verifies whether the `copy_and_aggregate` method of the `Pipeline` class can copy and aggregate staging Parquet files.

##### Test
The `copy_and_aggregate` method is called to copy and aggregate staging Parquet files. After execution, it is checked whether the aggregated Parquet file was created in the expected location.

##### Asserts
- Verifies if the aggregated Parquet file was correctly created at the specified location.

#### Conclusion
The automated tests implemented in the `TestPipeline` class provide comprehensive coverage of the functionalities of the `Pipeline` class. They ensure that the methods of the class function correctly in a variety of scenarios, helping to maintain code integrity and reliability.