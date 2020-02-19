# Calculate genre scores for every actor based on the movies they have acted in.
# The score is the average user score.

import pandas as pd
import numpy as np

actors = pd.read_csv("./name.tsv", sep="\t", header=0)
titles = pd.read_csv("./title.tsv", sep="\t", header=0)
ratings = pd.read_csv("./ratings.tsv", sep="\t", header=0)

row_i = 0

def calculate_genre_score(row):
    global row_i
    row_i += 1
    if row_i % 100 == 0:
        print(row_i)

    # Actor is known for
    find_titles = row.knownForTitles.split(",")
    # Get titles
    knownfor = titles.loc[titles.tconst.isin(find_titles)][["tconst", "genres"]]

    genre_scores = {}
    for _, row in knownfor.iterrows():
        r = ratings.loc[ratings.tconst == row.tconst].averageRating.iloc[0]
        gs = row.genres.split(",")
        for g in gs:
            genre_scores.setdefault(g, []).append(r)

    for g in genre_scores:
        genre_scores[g] = np.mean(genre_scores[g])

    return genre_scores

print(actors.shape)
actors["genre_score"] = actors.apply(calculate_genre_score, axis=1)

scores = actors[["nconst", "genre_score"]]
scores.to_csv("genre_scores.tsv", sep="\t")