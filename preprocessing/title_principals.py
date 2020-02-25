import pandas as pd
import numpy as np

print("Loading data")
data = pd.read_csv("../data/title.principals.tsv", sep="\t", header=0)
titles = pd.read_csv("../data/title.tsv", sep="\t", header=0, index_col=0)

print("Converting")
data.drop(columns=["ordering", "category", "job", "characters"], inplace=True)

data = data[data.tconst.isin(titles.index)]

# Should also filter actors from names?

print("Saving")
data.to_csv("../data/principals.tsv", sep="\t")