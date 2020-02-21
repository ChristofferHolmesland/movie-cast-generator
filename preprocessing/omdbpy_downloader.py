import pandas as pd
import omdb
from concurrent.futures import ThreadPoolExecutor

import time

data = pd.read_csv("../data/title.tsv", delimiter='\t', header=0, index_col=0)
ids = data.index.tolist()

api_keys = []
omdb_apis = [omdb.Api(apikey=key) for key in api_keys]

summaries = []
box_offices = []

def get_movie(id):
    mov = omdb_api.search(imdb_id=id).json()
    summaries.append((id, mov.get("Plot", "Missing")))
    box_offices.append((id, mov.get("BoxOffice", "Missing")))

workers = 10000

start_time = time.time()
for i in range(0, len(ids), workers):
    end_id = i + workers
    if end_id > len(ids): end_id = len(ids)

    print(f"Getting ids at index: {i}:{end_id}")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(get_movie, ids[i:end_id])
end_time = time.time()

print(end_time - start_time)

df_summary = pd.DataFrame(summaries, columns=["tconst", "summary"])
df_summary.set_index("tconst", drop=True, inplace=True)
df_box_office = pd.DataFrame(box_offices, columns=["tconst", "boxoffice"])
df_box_office.set_index("tconst", drop=True, inplace=True)

df = pd.concat([df_box_office, df_summary], axis=1)
df.to_csv("../data/summary_box_office.tsv", sep="\t")