import os, pathlib
import pandas as pd

# Set list path where you can store additional information or lists, if needed
dldir = pathlib.Path("download")
process_path = pathlib.Path("process")
verify_path = pathlib.Path("verify")

# Check if verification file exists
verification_success = verify_path / "verification_success.txt"
if not verification_success.is_file():
    print("Stop building because verification failed.")
    quit()

# Create brick directory to store Parquet files
brickpath = pathlib.Path("brick")
(brickpath / "temposeq.parquet").mkdir(parents=True, exist_ok=True)

# Build temposeq parquet files
# Read the list of temposeq files
file_paths = [line.strip() for line in open(dldir / "downloaded_files.txt", "r")]

# Process each temposeq file 
for inputpath in file_paths:
    outputpath = inputpath.replace("download", "brick")\
        .replace("temposeq", "temposeq.parquet")\
        .replace(".csv", ".parquet")
    
    pd.read_csv(inputpath).to_parquet(outputpath)

# Build pathways parquet file
inputpath = process_path / "pathways.csv"
outputpath = brickpath / inputpath.relative_to(process_path).with_suffix(".parquet")
pd.read_csv(inputpath).to_parquet(outputpath)
