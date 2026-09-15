import os
import requests

from dotenv import load_dotenv
from airflow.sdk import dag, task


# Load environment variables
load_dotenv("/home/ubuntu/airflow-project/.env")

URL = os.getenv("URL")
FILE_PATH = os.getenv("file_path")


@dag(
    dag_id="fetching_gitAPI_data",
    schedule=None,
    catchup=False
)
def fetching_gitAPI_data():

    @task.python
    def fetch_data_api():

        print("API URL:", URL)
        print("File path:", FILE_PATH)

        # Check environment variables
        if not URL:
            raise ValueError("URL is not set in .env file")

        if not FILE_PATH:
            raise ValueError("file_path is not set in .env file")

        # Call GitHub API/raw file
        response = requests.get(URL)

        # Raise error if request failed
        response.raise_for_status()

        # Create parent directory if it doesn't exist
        directory = os.path.dirname(FILE_PATH)
        os.makedirs(directory, exist_ok=True)

        # Save JSON file
        with open(FILE_PATH, "wb") as file:
            file.write(response.content)

        print(f"File successfully saved at: {FILE_PATH}")

        return FILE_PATH


    @task.python
    def conform_status(file_path):

        print(
            f"The API data has been successfully saved at: {file_path}"
        )


    # TaskFlow automatically creates dependency
    file_path = fetch_data_api()

    conform_status(file_path)


# Create DAG
fetching_gitAPI_data = fetching_gitAPI_data()