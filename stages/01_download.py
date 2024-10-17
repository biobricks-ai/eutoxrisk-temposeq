import json
import requests
import time
import pandas as pd
import os, pathlib
from tqdm import tqdm
from edelweiss_data import API, QueryExpression as Q

# Define paths and variables
filter_word = "EUT"
download_path = pathlib.Path("download")
dataset_path = download_path / "temposeq"
pathways_path = download_path / "pathways"
log_file_path = download_path / "downloaded_files.txt"
gene_set_path = download_path / "WikiPathway_2023_Human.gmt"
log_fold_change_threshold = 2
adjusted_p_value_threshold = 0.05

# Download the gene set file and write to file
r = requests.get("https://maayanlab.cloud/Enrichr/geneSetLibrary?mode=text&libraryName=WikiPathway_2023_Human")
with open(gene_set_path, "w") as f:
    f.write(r.text)

# Ensure directories exist
download_path.mkdir(parents=True, exist_ok=True)
dataset_path.mkdir(parents=True, exist_ok=True)
pathways_path.mkdir(parents=True, exist_ok=True)

# Initialize API and log file
api = API('https://api.develop.edelweiss.douglasconnect.com')
log_file = open(log_file_path, "w")

# get the dataset of interest
dataset = api.get_published_dataset(id='d8922983-1724-4ce8-af07-93f290d9c3c2', version='latest')
datasets = dataset.get_data(condition=None, aggregation_filters={}, order_by=None, ascending=None)
datasets = datasets[datasets['Filename'].str.contains(filter_word)].reset_index(drop=True)

# process overview
file_path = download_path / 'overview.csv'
datasets.to_csv(file_path, index=False)
log_file.write(str(file_path) + "\n")

# process individual datasets
for dataset_id in tqdm(datasets['Dataset id'], desc="Download individual datasets"):
    file_path = dataset_path / f'{dataset_id}.csv'
    dataset = api.get_published_dataset(id=dataset_id, version='latest')
    data_frame = dataset.get_data()
    data_frame.to_csv(file_path, index=False)
    log_file.write(str(file_path) + "\n")
