import pandas as pd
import pathlib
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import gseapy as gp

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define paths and variables
# Input
download_path = pathlib.Path("download")
dataset_path = download_path / "temposeq"
downloaded_file_path = download_path / "downloaded_files.txt"
overview_file_path = download_path / "overview.csv"
gene_set_path = download_path / "WikiPathway_2023_Human.gmt"

# Output
process_path = pathlib.Path("process")
log_file_path = process_path / "processed_files.txt"
pathways_file_path = process_path / "pathways.csv"

LOG_FOLD_CHANGE_THRESHOLD = 2
ADJUSTED_P_VALUE_THRESHOLD = 0.05
GENE_SET_LIBRARY = str(gene_set_path)

# Ensure directories exist
dataset_path.mkdir(parents=True, exist_ok=True)
process_path.mkdir(parents=True, exist_ok=True)

# Read the overview dataset
datasets = pd.read_csv(overview_file_path)

# Check that all temposeq datasets have been downloaded in the previous step
downloaded_file = open(downloaded_file_path, "r")
downloaded_files = downloaded_file.read().splitlines()
downloaded_files = downloaded_files[1 : ]  # remove the first file, which is the overview file

if len(downloaded_files) == datasets.shape[0]:
    print("All temposeq datasets have been downloaded. Proceed with processing.")
else:
    print("""Number of temposeq datasets and downloaded files do not match.\n
          Number of temposeq datasets: {0}\n
          Number of downloaded files: {1}""".format(datasets.shape[0], len(downloaded_files)))
    quit()


# Initiate the log file that will store processed filepaths
log_file = open(log_file_path, "w")

degs_data_frame = pd.DataFrame()

# Loop over downloaded temposeq files and identify deg and process the pathways
for dataset_id in tqdm(datasets['Dataset id'], desc="Collecting de genes"):
    temposeq_path = dataset_path / f'{dataset_id}.csv'
    data_frame = pd.read_csv(temposeq_path)
    
    is_significant_fold_change = (data_frame['logFC'] > LOG_FOLD_CHANGE_THRESHOLD) | \
                                    (data_frame['logFC'] < -LOG_FOLD_CHANGE_THRESHOLD)
    
    is_significant_p_value = data_frame['padj'] < ADJUSTED_P_VALUE_THRESHOLD
    
    tmp_degs_data_frame = data_frame[is_significant_fold_change & is_significant_p_value][['SYMBOL']]
    degs_data_frame = pd.concat([degs_data_frame, tmp_degs_data_frame], axis=0, ignore_index=True)

    log_file.write(str(temposeq_path) + "\n")

degs_data_frame = degs_data_frame.drop_duplicates(subset=['SYMBOL'])

# process pathways
def get_pathways_for_gene(gene_symbol, gene_set_library):
    enr = gp.enrich(gene_list=[gene_symbol],
                    gene_sets=gene_set_library,
                    background=None,
                    outdir=None,
                    verbose=False)
    return enr.results

# Process deg genes
degs_pathways = pd.DataFrame()
count_skipped = 0
with logging_redirect_tqdm():
    for _, gene_row in tqdm(degs_data_frame.iterrows(), desc=f"Processing {degs_data_frame.shape[0]} de genes"):

        if pd.isna(gene_row['SYMBOL']):
            continue

        if gene_row['SYMBOL'] == "":
            continue

        try:
            pathways = get_pathways_for_gene(gene_row['SYMBOL'], GENE_SET_LIBRARY)
        except ValueError:
            count_skipped += 1
            LOG.info(f"Skipping gene {gene_row['SYMBOL']} (# skipped: {count_skipped})")
            continue

        if isinstance(pathways, pd.DataFrame):
            if not pathways.empty:
                degs_pathways = pd.concat([degs_pathways, pathways], ignore_index=True)
    
degs_pathways.to_csv(pathways_file_path, index=False)
