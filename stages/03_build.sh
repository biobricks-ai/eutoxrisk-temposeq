#!/usr/bin/env bash

# Script to process CSV files and build Parquet files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Define the download directory
downloadpath="$localpath/download"
echo "Download path: $downloadpath"

# Set list path where you can store additional information or lists, if needed
listpath="$localpath/list"
echo "List path: $listpath"

# Create brick directory to store Parquet files
export brickpath="$localpath/brick"
mkdir -p $brickpath
echo "Brick path: $brickpath"

# Process CSV files and create Parquet files in parallel
# Calling a Python script with arguments input CSV and output Parquet filenames
for file in "$downloadpath"/*.csv; do
  filename=$(basename "$file" .csv)
  inputpath="$file"
  outputpath="$brickpath/$filename.parquet"
  echo "$inputpath"
  echo "$outputpath"
  python stages/csv2parquet.py "$inputpath" "$outputpath"
done


echo "CSV to Parquet conversion done."