# Removes ratings for titles which are not in the title.tsv file
# Run it after preprocess_title_basics.py

import pandas as pd
import numpy as np

print("Loading data")
data = pd.read_csv("../data/title.ratings.tsv", sep="\t", header=0, index_col=0)

invalid_indexes = data[data.numVotes < 50].index
data.drop(invalid_indexes, inplace=True)

print("Saving")
data.to_csv("../data/ratings.tsv", sep="\t")