import time
from concurrent.futures import ThreadPoolExecutor

import omdb
import pandas as pd

#  Maximum number of threads to start
workers = 10000

data = pd.read_csv("../data/title.tsv", delimiter='\t', header=0, index_col=0)
ids = data.index.tolist()

api_keys = []
omdb_apis = [omdb.Api(apikey=key) for key in api_keys]

summaries = []
box_offices = []

# Divide the ids between the api keys
n_ids = len(ids)
id_slices = []
ids_per_key = n_ids / len(api_keys)
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

        print(f"Getting ids at index: {j}:{end_id}")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(get_movie, omdb_apis[i], api_ids[j:end_id])
    end_time = time.time()
    print(f"API_KEY={api_keys[i]} finished in {end_time - start_time}")

last_time = time.time()

print(f"Total time: {last_time - first_time}")

df_summary = pd.DataFrame(summaries, columns=["tconst", "summary"])
df_summary.set_index("tconst", drop=True, inplace=True)
df_box_office = pd.DataFrame(box_offices, columns=["tconst", "boxoffice"])
df_box_office.set_index("tconst", drop=True, inplace=True)

df = pd.concat([df_box_office, df_summary], axis=1)
df.to_csv("../data/summary_box_office.tsv", sep="\t")