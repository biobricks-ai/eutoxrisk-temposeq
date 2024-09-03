import os, pathlib
import pandas as pd

# Set list path where you can store additional information or lists, if needed
dldir = pathlib.Path("download")

# Create brick directory to store Parquet files
brickpath = pathlib.Path("brick")
(brickpath / "temposeq.parquet").mkdir(parents=True, exist_ok=True)
(brickpath / "pathways.parquet").mkdir(parents=True, exist_ok=True)

# Read the list of input files
file_paths = [line.strip() for line in open(dldir / "files.txt", "r")]

# Process each file path
for inputpath in file_paths:
    outputpath = inputpath.replace("download", "brick")\
        .replace("temposeq", "temposeq.parquet")\
        .replace("pathways", "pathways.parquet")\
        .replace(".csv", ".parquet")
    
    pd.read_csv(inputpath).to_parquet(outputpath)

