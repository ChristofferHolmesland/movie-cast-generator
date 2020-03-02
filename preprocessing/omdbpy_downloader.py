import math
import time
from concurrent.futures import ThreadPoolExecutor

import omdb
import pandas as pd

import sys

# Runtime 6256.2548 seconds

#  Maximum number of threads to start
workers = 10000
min_requests_per_key = 500
api_file = "../secrets/omdb_api_keys.secret"
input_file = "../data/title.tsv"
output_file = "../data/summary_box_office.tsv"

data = pd.read_csv(input_file, delimiter='\t', header=0, index_col=0)
ids = data.index.tolist()

api_keys = []
with open(api_file, "r") as f:
    api_keys = [line.strip() for line in f.readlines() if line != "!DOCTYPE html>"]
api_keys = list(set(api_keys))

omdb_apis = [omdb.Api(apikey=key) for key in api_keys]

summaries = []
box_offices = []

# Divide the ids between the api keys
n_ids = len(ids)
id_slices = []
ids_per_key = math.ceil(n_ids / len(api_keys))
if ids_per_key < min_requests_per_key:
    ids_per_key = min_requests_per_key

print(f"{ids_per_key} assigned to each API key")

for i in range(0, n_ids, ids_per_key):
    end_id = i + ids_per_key
    if end_id > n_ids: end_id = n_ids
    id_slices.append(ids[i:end_id])

def get_movie(api, id):
    mov = api.search(imdb_id=id).json()
    summaries.append((id, mov.get("Plot", "Missing")))
    box_offices.append((id, mov.get("BoxOffice", "Missing")))

first_time = time.time()

for i in range(len(id_slices)):
    start_time = time.time()
    api_ids = id_slices[i]
    for j in range(0, len(api_ids), workers):
        end_id = j + workers
        if end_id > len(api_ids): end_id = len(api_ids)

        print(f"Getting ids at index: {i*ids_per_key+j}:{i*ids_per_key+end_id}")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(get_movie, [omdb_apis[i]]*len(api_ids[j:end_id]), api_ids[j:end_id])

    df_summary = pd.DataFrame(summaries, columns=["tconst", "summary"])
    df_summary.set_index("tconst", drop=True, inplace=True)
    df_box_office = pd.DataFrame(box_offices, columns=["tconst", "boxoffice"])
    df_box_office.set_index("tconst", drop=True, inplace=True)

    df = pd.concat([df_box_office, df_summary], axis=1)
    
    write_header = i == 0
    df.to_csv(output_file, mode="a", sep="\t", header=write_header)
    
    summaries = []
    box_offices = []
    end_time = time.time()
    print(f"API_KEY={api_keys[i]} finished in {end_time - start_time}")

last_time = time.time()

print(f"Total time: {last_time - first_time}")