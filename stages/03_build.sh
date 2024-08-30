#!/usr/bin/env bash

# Script to process CSV files and build Parquet files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Set list path where you can store additional information or lists, if needed
listpath="$localpath/list"
echo "List path: $listpath"

# Create brick directory to store Parquet files
export brickpath="$localpath/brick"
mkdir -p $brickpath
echo "Brick path: $brickpath"

mkdir -p "$brickpath/temposeq.parquet"
mkdir -p "$brickpath/pathways.parquet"

# Loop through each line in files.txt, which contains paths to the .csv files
while read inputpath; do
  # Create output path by replacing segments in the file path
  outputpath=$(echo "$inputpath" | sed -e 's/download/brick/' -e 's/temposeq/temposeq.parquet/' -e 's/pathways/pathways.parquet/' -e 's/\.csv/\.parquet/')

  # Print paths for verification
  echo "Input path: $inputpath"
  echo "Output path: $outputpath"

  # Call the Python script for converting CSV to Parquet
  python stages/csv2parquet.py "$inputpath" "$outputpath"
done < "$listpath/files.txt"

echo "CSV to Parquet conversion done."