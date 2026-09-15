import os
import pandas as pd

from dotenv import load_dotenv
from airflow.sdk import dag, task


load_dotenv("/home/ubuntu/airflow-project/.env")

URL = os.getenv("URL")
FILE_PATH = os.getenv("file_path")
PROCESSED_PATH = os.getenv("processed_path")


@dag(
    dag_id="processing_data_with_pandas",
    schedule=None,
    catchup=False
)
def processing_data_with_pandas():

    @task.python
    def fetch_data_api():

        os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

        df = pd.read_json(URL)

        df.to_json(
            FILE_PATH,
            orient="records",
            indent=4
        )

        print(f"Raw data saved at: {FILE_PATH}")

        return FILE_PATH


    @task.python
    def process_data(file_path):

        # Read the downloaded JSON
        df = pd.read_json(file_path)

        print("Input data:")
        print(df.head())

        # Your Pandas transformations go here


        # Create processed_data folder
        os.makedirs(
            os.path.dirname(PROCESSED_PATH),
            exist_ok=True
        )

        # Save processed data
        df.to_json(
            PROCESSED_PATH,
            orient="records",
            indent=4
        )

        print(
            f"Processed data saved at: {PROCESSED_PATH}"
        )

        return PROCESSED_PATH


    @task.python
    def conform_status(processed_path):

        print("Processing completed.")
        print(
            f"File saved at: {processed_path}"
        )


    # Dependencies
    raw_file = fetch_data_api()

    processed_file = process_data(raw_file)

    conform_status(processed_file)


processing_data_with_pandas()