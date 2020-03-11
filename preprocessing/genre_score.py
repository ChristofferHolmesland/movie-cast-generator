import pandas as pd
import numpy as np

principals = pd.read_csv("../data/principals.tsv", sep="\t", header=0)
titles = pd.read_csv("../data/title.tsv", sep="\t", header=0, index_col=0)
ratings = pd.read_csv("../data/ratings.tsv", sep="\t", header=0, index_col=0)

title_genre_ratings = titles.join(ratings, how="inner")

genre_ratings = \
    title_genre_ratings.apply(lambda row: {genre: (row.averageRating, row.numVotes) for genre in row.genres.split(",")}, axis=1)

row_i = 0

def calc_genre_score(row):
    global row_i
    row_i += 1
    if row_i % 10000 == 0:
        print(row_i)

    row = list(row)

    score = {}
    for movie in row:
        movie_ratings = genre_ratings.get(movie, default=None)
        if movie_ratings == None: continue

        for key, value in movie_ratings.items():
            if key not in score:
                score[key] = []
            score[key].append(value)

    for key, value in score.items():
        sum_votes = sum([v[1] for v in value])
        score[key] = sum([v[1]/sum_votes * v[0] for v in value])

    return str(score)

actor_scores = principals.groupby(["nconst"])["tconst"].apply(calc_genre_score)

actor_scores.replace("{}", np.nan, inplace=True)
actor_scores.dropna(inplace=True)
actor_scores.rename("genre_score", inplace=True)

actor_scores.to_csv("../data/genre_scores.tsv", sep="\t", header=True)