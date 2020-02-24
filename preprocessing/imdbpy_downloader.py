import pandas as pd
from imdb import IMDb
from concurrent.futures import ThreadPoolExecutor

import time

data = pd.read_csv("../title.tsv", delimiter='\t', header=0, index_col=0)
ids = [id[2:] for id in data.index]

im = IMDb()

summaries = []
box_offices = []

def get_movie(id):
    mov = im.get_movie(id)
    
    if "plot" in mov and len(mov["plot"]) > 0:
        summaries.append((id, mov["plot"][0].split("::")[0]))
    else:
        summaries.append((id, None))

    box_offices.append((id, mov.get("box office", None)))

start_time = time.time()
with ThreadPoolExecutor(max_workers=1000) as executor:
    executor.map(get_movie, ids[:1000])
end_time = time.time()

print(end_time - start_time)
print(summaries)
print(box_offices)