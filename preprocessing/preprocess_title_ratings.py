# Removes ratings for titles which are not in the title.tsv file
# Run it after preprocess_title_basics.py

import pandas as pd
import numpy as np

print("Loading data")
data = pd.read_csv("./title.ratings.tsv", sep="\t", header=0)
titles = pd.read_csv("./title.tsv", sep="\t", header=0)

print("Converting")
data[~titles["tconst"].isin(data["tconst"])].dropna(inplace=True)

print(data.shape)
print(titles.shape)

print("Saving")
data.to_csv("ratings.tsv", sep="\t")