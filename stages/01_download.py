import json
import requests
import time
import pandas as pd
import os, pathlib
from tqdm import tqdm
from edelweiss_data import API, QueryExpression as Q

# Define paths and variables
filter_word = "EUT"
overview_path = pathlib.Path("download")
dataset_path = overview_path / "temposeq"
pathways_path = overview_path / "pathways"
log_file_path = overview_path / "files.txt"
log_fold_change_threshold = 2
adjusted_p_value_threshold = 0.05

# Ensure directories exist
overview_path.mkdir(parents=True, exist_ok=True)
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
file_path = overview_path / 'overview.csv'
datasets.to_csv(file_path, index=False)
log_file.write(str(file_path) + "\n")

# process individual datasets
for dataset_id in tqdm(datasets['Dataset id'], desc="Processing individual datasets"):
    file_path = dataset_path / f'{dataset_id}.csv'
    dataset = api.get_published_dataset(id=dataset_id, version='latest')
    data_frame = dataset.get_data()
    data_frame.to_csv(file_path, index=False)
    log_file.write(str(file_path) + "\n")

# process pathways
def get_pathways_for_gene(gene_symbol):
    """Fetch pathways associated with a gene symbol using the Enrichr API."""
    ENRICHR_URL = "https://amp.pharm.mssm.edu/Enrichr/addList"
    ENRICHR_URL_A = 'https://amp.pharm.mssm.edu/Enrichr/enrich?userListId=%s&backgroundType=%s'
    GENE_SET_LIBRARY = "KEGG_2015"
    
    payload = { 'list': (None, str(gene_symbol)), 'description': (None, "No description") }
    response = requests.post(url=ENRICHR_URL, files=payload)
    response.raise_for_status()
    
    user_list_id = json.loads(response.text)['userListId']
    
    time.sleep(2)
    
    response_gene_list = requests.get(ENRICHR_URL_A % (str(user_list_id), GENE_SET_LIBRARY))
    response_gene_list.raise_for_status()
    
    enrichr_results = json.loads(response_gene_list.text)[GENE_SET_LIBRARY]
    
    return pd.DataFrame([{
        'Gene symbol': gene_symbol,
        'Rank': result[0],
        'Pathway': result[1],
        'p-value': result[2],
        'Adj. p-value': result[6],
        'Odds Ratio': result[3],
        'Combined score': result[4]
    } for result in enrichr_results])

columns = ['Gene symbol', 'Rank', 'Pathway', 'p-value', 'Adj. p-value', 'Odds Ratio', 'Combined score']
degs_pathways = pd.DataFrame(columns=columns)

for dataset_id in tqdm(datasets['Dataset id'], desc="Processing pathways"):
    file_path = pathways_path / f'{dataset_id}_pathways.csv'
    dataset = api.get_published_dataset(id=dataset_id, version='latest')
    data_frame = dataset.get_data()
    
    is_significant_fold_change = (data_frame['logFC'] > log_fold_change_threshold) | \
                                    (data_frame['logFC'] < -log_fold_change_threshold)
    is_significant_p_value = data_frame['padj'] < adjusted_p_value_threshold
    
    degs_data_frame = data_frame[is_significant_fold_change & is_significant_p_value]
    
    if not degs_data_frame.empty:
        for _, gene_row in degs_data_frame.iterrows():
            pathways = get_pathways_for_gene(gene_row['SYMBOL'])
            degs_pathways = pd.concat([degs_pathways, pathways], ignore_index=True)
            
        degs_pathways.to_csv(file_path, index=False)
        log_file.write(str(file_path) + "\n")
    else:
        print(f"Missing required columns in dataset {dataset_id}")
