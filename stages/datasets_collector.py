import json
import requests
import time
import pandas as pd
import os
import sys
from edelweiss_data import API, QueryExpression as Q

class DatasetProcessor:
    """
    A class to process datasets, filter and download data, and analyze gene pathways.

    Attributes:
        filter_word (str): Word used to filter datasets.
        overview_path (str): Path to save overview data.
        dataset_path (str): Path to save individual dataset CSVs.
        pathways_path (str): Path to save pathway analysis results.
        log_file_path (str): Full path to the log file where paths of saved files are stored.
    """

    def __init__(self, filter_word, overview_path, dataset_path, pathways_path, log_file_path):
        """
        Initialize the DatasetProcessor with paths and API settings.

        Args:
            filter_word (str): The keyword to filter datasets.
            overview_path (str): The directory path to save overview data.
            dataset_path (str): The directory path to save individual datasets.
            pathways_path (str): The directory path to save pathways data.
            log_file_path (str): The full path to the log file.
        """
        self.filter_word = filter_word
        self.overview_path = overview_path
        self.dataset_path = dataset_path
        self.pathways_path = pathways_path
        self.log_file_path = log_file_path
        self.api = API('https://api.develop.edelweiss.douglasconnect.com')
        self.log_fold_change_threshold = 2
        self.adjusted_p_value_threshold = 0.05
        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        self.log_file = open(f"{self.log_file_path}/files.txt", "w")

    def get_pathways_for_gene(self, gene_symbol):
        """
        Fetch pathways associated with a gene symbol using the Enrichr API.

        Args:
            gene_symbol (str): The gene symbol to query pathways for.

        Returns:
            DataFrame: Pathways data for the gene symbol.
        """
        # API endpoints for adding and querying lists
        ENRICHR_URL = "https://amp.pharm.mssm.edu/Enrichr/addList"
        ENRICHR_URL_A = 'https://amp.pharm.mssm.edu/Enrichr/enrich?userListId=%s&backgroundType=%s'
        GENE_SET_LIBRARY = "KEGG_2015"

        # Prepare payload for the POST request to add a gene list
        payload = {
            'list': (None, str(gene_symbol)),
            'description': (None, "No description")
        }

        # Post the gene list and handle the response
        response = requests.post(url=ENRICHR_URL, files=payload)
        if response.status_code != 200:
            raise Exception(f"Error: {response.text}")

        job_id = json.loads(response.text)
        user_list_id = job_id['userListId']

        # Delay to allow processing
        time.sleep(2)

        # Get the enrich results from Enrichr
        response_gene_list = requests.get(ENRICHR_URL_A % (str(user_list_id), GENE_SET_LIBRARY))
        if response_gene_list.status_code != 200:
            raise Exception(f"Error: {response_gene_list.text}")

        enrichr_data = json.loads(response_gene_list.text)
        enrichr_results = enrichr_data[GENE_SET_LIBRARY]

        # Create data entries for each pathway result
        data = [{'Gene symbol': gene_symbol,
                 'Rank': result[0],
                 'Pathway': result[1],
                 'p-value': result[2],
                 'Adj. p-value': result[6],
                 'Odds Ratio': result[3],
                 'Combined score': result[4]} for result in enrichr_results]

        return pd.DataFrame(data)

    def process_overview(self, datasets):
        """
        Save the overview of datasets to a CSV file and log the file path.
        """
        os.makedirs(self.overview_path, exist_ok=True)
        file_path = f'{self.overview_path}/overview.csv'
        datasets.to_csv(file_path, index=False)
        self.log_file.write(file_path + "\n")

    def process_individual_datasets(self, datasets):
        """
        Process and save individual datasets as CSV files and log each file path.
        """
        os.makedirs(self.dataset_path, exist_ok=True)
        for dataset_id in datasets['Dataset id']:
            file_path = f'{self.dataset_path}/{dataset_id}.csv'
            dataset = self.api.get_published_dataset(id=dataset_id, version='latest')
            data_frame = dataset.get_data()
            data_frame.to_csv(file_path, index=False)
            self.log_file.write(file_path + "\n")

    def process_pathways(self, datasets):
        """
        Process gene pathways for datasets, save results, and log each file path.
        """
        os.makedirs(self.pathways_path, exist_ok=True)
        for dataset_id in datasets['Dataset id']:
            file_path = f'{self.pathways_path}/{dataset_id}_pathways.csv'
            dataset = self.api.get_published_dataset(id=dataset_id, version='latest')
            data_frame = dataset.get_data()
            pathways = self.get_pathways_for_gene(data_frame['SYMBOL'].iloc[0])
            if len(pathways) > 0:
                pathways.to_csv(file_path, index=False)
                self.log_file.write(file_path + "\n")

    def process_datasets(self):
        """
        Main method to process all datasets based on the specified filters and paths.
        """
        dataset = self.api.get_published_dataset(
            id = 'd8922983-1724-4ce8-af07-93f290d9c3c2',
            version = 'latest',
            )

        datasets = dataset.get_data(
            condition = None,
            aggregation_filters = {},
            order_by = None,
            ascending = None,
            )

        datasets = datasets[datasets['Filename'].apply(lambda x: self.filter_word in str(x))].reset_index(drop=True)

        self.process_overview(datasets)
        self.process_individual_datasets(datasets)
        self.process_pathways(datasets)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Usage: python script.py <filter_word> <overview_path> <dataset_path> <pathways_path> <log_file_path>")
    else:
        processor = DatasetProcessor(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        processor.process_datasets()
