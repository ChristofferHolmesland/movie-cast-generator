# Processes name.basics.tsv into name.tsv
# Dead people are removed
# Gender is determined
# People who are not actor/actress are removed.

import pandas as pd
import numpy as np

print("Loading data")
data = pd.read_csv("./name.basics.tsv", sep="\t", header=0, index_col=0)

print("Converting")
def find_gender(row):
    if "actor" in row:
        return 0
    if "actress" in row:
        return 1
    return np.nan

def remove_missing_value(row):
    if row == "\\N":
        return np.nan
    return row

def remove_non_missing_value(row):
    if row != "\\N":
        return np.nan
    return row

# Remove dead people
data["deathYear"] = data["deathYear"].apply(remove_non_missing_value)
# Remove people who are not known for any titles
data["knownForTitles"] = data["knownForTitles"].apply(remove_missing_value)
# Remove people with missing birth year
data["birthYear"] = data["birthYear"].apply(remove_missing_value)
data.dropna(subset=("primaryProfession", "knownForTitles", "birthYear", "deathYear"), inplace=True)
# Determine gender from their professions
data["gender"] = data["primaryProfession"].apply(find_gender)
# Remove people who are not actor or actress
data.dropna(subset=("gender", ), inplace=True)
data.drop(columns="primaryProfession", inplace=True)

print("Saving")
data.to_csv("name.tsv", sep="\t")