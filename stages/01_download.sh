#!/usr/bin/env bash

# Script to download CSV files from Edelweiss API and maintain a list of downloaded files

# Get local path
localpath=$(pwd)
echo "Local path: $localpath"

# Define the list directory to save list of downloaded files
listpath="$localpath/list"
echo "List path: $listpath"
mkdir -p $listpath

# Define the download directory
downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"


# create the sub-directories under the download directory

mkdir -p "$downloadpath/temposeq"
mkdir -p "$downloadpath/pathways"

# Change directory to download path
cd stages

# Install the edelweiss_data Python package
pip install edelweiss-data

# Python code starts here
python datasets_collector.py TEMPO $downloadpath "$downloadpath/temposeq" "$downloadpath/pathways" $listpath
