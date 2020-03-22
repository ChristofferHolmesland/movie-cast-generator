import os
import re
import ast
import sys

import numpy as np
import pandas as pd
#import tensorflow as tf
#import tensorflow_hub as hub

# Relative file paths
script_path = os.path.dirname(__file__)
input_file = os.path.join(script_path, "../input2.txt")
genre_file = os.path.join(script_path, "../mrjob_data/genre_scores.tsv")
actor_file = os.path.join(script_path, "../data/name.tsv")
summary_file = os.path.join(script_path, "../data/summary_box_office.tsv")
principals_file = os.path.join(script_path, "../data/principals.tsv")
titles_file = os.path.join(script_path, "../data/title.tsv")

# Load similarity model from tfhub
model_url = "https://tfhub.dev/google/universal-sentence-encoder/4" 
#sim_model = hub.load(model_url)

# Load data from files
genre_data = pd.read_csv(genre_file, sep="\t", header=0, index_col=0)
actor_data = pd.read_csv(actor_file, sep="\t", header=0, index_col=0)
summary_data = pd.read_csv(summary_file, sep="\t", header=0, index_col=0)
principals_data = pd.read_csv(principals_file, sep="\t", header=0)
titles_file = pd.read_csv(titles_file, sep="\t", header=0, index_col=0)

actor_data["age"] = 2020 - actor_data["birthYear"]

genres = []
actor_descriptions = []
plot = ""

with open(input_file, "r") as f:
    genres_raw = f.readline().split(":")[1]
    genres = [genre.strip() for genre in genres_raw.split(",") if len(genre.strip()) > 0]
    actors_raw = f.readline().split(":")[1]
    actor_descriptions = [actor.strip().replace(",", "") for actor in actors_raw.split(";") if len(actor.strip()) > 0]
    plot_raw = f.readline().split(":")[1:]
    plot = (":".join(plot_raw)).strip()

actor_descriptions = [[descr.split(" ")[0], *descr.split(" ")[1].split("-")] for descr in actor_descriptions]
for a in actor_descriptions:
    if len(a) == 2:
        a.append(a[1])
    a[0] = 1 if a[0] == "Female" else 0
    a[1] = int(a[1])
    a[2] = int(a[2])

actor_data = actor_data.join(genre_data, how="inner", on="nconst")

print(genres)

def calc_average_genre_score(scores):
    scores = ast.literal_eval(scores)
    score = [scores[s] for s in scores if s in genres]
    if len(score) != len(genres):
        return np.nan

    return np.mean(score)

print(principals_data)

candidates = []
for desc in actor_descriptions:
    cond = actor_data["gender"] == desc[0]
    cond &= actor_data["age"] >= desc[1]
    cond &= actor_data["age"] <= desc[2]
    cand = actor_data.loc[cond].copy().drop(columns=["birthYear", "deathYear", "knownForTitles", "gender"])
    cand["average_genre_score"] = cand["genre_score"].apply(calc_average_genre_score)
    cand.dropna(inplace=True, subset=["average_genre_score"])

del actor_data