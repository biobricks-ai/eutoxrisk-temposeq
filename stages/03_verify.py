import pandas as pd
import pathlib

# Define paths and variables
overview_path = pathlib.Path("download")
downloaded_file_path = overview_path / "downloaded_files.txt"
processed_file_path = overview_path / "processed_files.txt"
overview_file_path = overview_path / "overview.csv"
verification_file_path = overview_path / "verification_success.txt"

# Check that verification file does not exist
try:
    verification_file_path.unlink()
    print('Verification file deleted')
except:
    print("Verification file doesn't exist")


# Read the overview dataset
overview_file_path = overview_path / 'overview.csv'
datasets = pd.read_csv(overview_file_path)

# Read the downloaded file list
downloaded_file = open(downloaded_file_path, "r")
downloaded_files = downloaded_file.read().splitlines()
downloaded_files = downloaded_files[1 : ]  # remove the first file, which is the overview file

# Read the processed file list
processed_file = open(processed_file_path, "r")
processed_files = processed_file.read().splitlines()

# Check that all temposeq datasets have been downloaded
if len(downloaded_files) == datasets.shape[0]:
    print("Verification step 1: OK.\nAll temposeq datasets have been downloaded.")
else:
    print("""Verification failed.\nNumber of temposeq datasets and downloaded files do not match.\n
          Number of temposeq datasets: {0}\n
          Number of downloaded files: {1}""".format(datasets.shape[0], len(downloaded_files)))
    quit()

# Check that all temposeq datasets have been downloaded
if len(processed_files) == datasets.shape[0]:
    print("Verification step 2: OK.\nAll temposeq datasets have been processed.")
else:
    print("""Verification failed.\nNumber of temposeq datasets and processed files do not match.\n
          Number of temposeq datasets: {0}\n
          Number of processed files: {1}""".format(datasets.shape[0], len(processed_files)))
    quit()

log_file = open(verification_file_path, "w")
log_file.write(f"Verification successful.")