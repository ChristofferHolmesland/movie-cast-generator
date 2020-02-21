import time
from concurrent.futures import ThreadPoolExecutor

import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

imdb = "https://www.imdb.com/title/tt{0}"

result = []

def scrape(title_id):
    print(title_id)
    r = requests.get(imdb.format(str(title_id).zfill(7)))
    soup = BeautifulSoup(r.text, "html.parser")
    result.append(soup.find("div", class_="summary_text").contents[0].strip())

start_time = time.time()

with ThreadPoolExecutor(max_workers=100) as executor:
    for i in range(10):
        executor.map(scrape, range(1, 101))

end_time = time.time()

#print(end_time - start_time)