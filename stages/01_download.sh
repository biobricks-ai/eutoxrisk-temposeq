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




# # Note: Using python3 might be python or python3.7 etc., depending on your system configuration
# python3 << END

# from edelweiss_data import API, QueryExpression as Q
# import os

# api = API('https://api.develop.edelweiss.douglasconnect.com')

# datasets = api.get_published_datasets(
#   condition = None,
#   aggregation_filters = {},
#   order_by = None,
#   ascending = None,
# )

# datasets.reset_index(inplace=True, drop=True)

# # Filter based on the 'name' attribute
# datasets = datasets[datasets['dataset'].apply(lambda x: 'TEMPO' in str(x))].reset_index(drop=True)

# list_file_path = os.path.join('$listpath', 'files.txt')
# with open(list_file_path, 'w') as list_file:
#     for row in datasets['dataset']:
#         dataset = api.get_published_dataset(
#         id = row.id,
#         version = 'latest',  # Change to 'latest' to always get the latest version
#         )

#         data_frame = dataset.get_data(
#         condition = None,
#         aggregation_filters = {},
#         order_by = None,
#         ascending = None,
#         )
#         # os.makedirs('temposeq', exist_ok=True)
#         filename = f'./temposeq/{row.id}.csv'
#         data_frame.to_csv(filename, index=False)
#         # list_file.write(filename + '\n')

# END

# echo "Download from EdelweissData done."