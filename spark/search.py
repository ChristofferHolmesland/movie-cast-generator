import sys
import os
import ast
import pwd

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf, col, mean, array_contains, struct
import pyspark.sql.types as sqltypes

worker_module_path = "/home/ubuntu/.local/lib/python3.5/site-packages/"

sim_model_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
sim_model = hub.load(sim_model_url)

# Read input from environment variables.
search_genres = os.environ["search_genres"].split(",")
num_search_genres = len(search_genres)
search_actors = [[descr.split(" ")[0], *descr.split(" ")[1].split("-")] for descr in os.environ["search_actors"].split(",")]
for a in search_actors:
    if len(a) == 2:
        a.append(a[1])
search_plot = os.environ["search_plot"]

# Connect to spark.
spark = SparkSession.builder.appName("PySpark Search").getOrCreate()

# Read actors froms file to DataFrame, and calculate their age.
actors = spark.read.csv("project/spark/actors.tsv/part*", header=True, sep="\t")
actors = actors.withColumn("age", expr("2020 - birthYear"))

# Read movies from file to DataFrame.
movies = spark.read.csv("project/spark/movies.tsv/part*", header=True, sep="\t")

# Create views to enable SQL statements.
actors.createOrReplaceTempView("Actors")
movies.createOrReplaceTempView("Movies")

movie_sql = "SELECT tconst, summary from Movies WHERE summary != 'N/A'"
candidate_sql = "SELECT nconst, genre_score, tconst FROM Actors WHERE age >= {0} AND age <= {1} AND gender= {2}"

# Remove movies without a summary
movies = spark.sql(movie_sql)

# Decorator indicating that calc_average_score is an user-defined-function returning a float value.
@udf("float")
def calc_average_score(genre_score):
    score = 0
    genre_score = ast.literal_eval(genre_score)
    for genre in search_genres:
        score += genre_score.get(genre, -1000)
    return score / num_search_genres

# Pre allocate arrays
scores = [None] * movies.count()
sim_arr = [search_plot, None]

# Iterate over every movie summary and calculate the similarity score
for i, row in enumerate(movies.rdd.toLocalIterator()):
    sim_arr[1] = row.summary
    sim = sim_model(sim_arr)
    scores[i] = (row.tconst, 10 * float(np.dot(sim[0], sim[1])))

# Convert similarity scores to a dataframe and delete the local version
# of the scores.
sim_scores = spark.createDataFrame(scores, ["tconst", "sim_score"])
del scores

movies = movies \
    .join(sim_scores, movies.tconst == sim_scores.tconst) \
    .drop(sim_scores.tconst) \
    .orderBy(["sim_score"], ascending=False)

@udf("array<string>")
def convert_to_arr(tconsts):
    return tconsts[1:-1].split(", ")

candidates = []
for i, desc in enumerate(search_actors):
    # Select actors based on the actor description
    cand = spark.sql(candidate_sql.format(desc[1], desc[2], 1 if desc[0] == "Female" else 0))
    
    # Create condition that will only select actors with all of the genres.
    genre_condition = cand.genre_score.contains(search_genres[0])
    if len(search_genres) > 1:
        for genre in search_genres[1:]:
            genre_condition &= cand.genre_score.contains(genre)


    cand = cand.filter(genre_condition).withColumn("tconst", convert_to_arr(cand.tconst))
    cand = cand.join(movies, array_contains(cand.tconst, movies.tconst)).drop(movies.tconst)
    d = cand.groupby("nconst").agg({"sim_score": "max"})
    cand = cand.join(d, cand.nconst == d.nconst).drop(d.nconst)
    cand = cand.select("nconst", calc_average_score("genre_score").alias("avg_genre_score"), col("max(sim_score)").alias("max_sim_score"))
    cand = cand.withColumn("score", (cand.avg_genre_score + cand.max_sim_score) / 2)
    cand = cand.drop_duplicates(["nconst"]).orderBy(["score"], ascending=False)

    candidates.append(cand)

    # Save candidates to disk
    cand.write.csv("project/spark/candidates{}.tsv/".format(i), sep="\t", header=True)

# Save moveis to disk
movies.write.csv("project/spark/movies_score.tsv/", sep="\t", header=True)

# Number of actors fitting the Intersteller search
# C0: 704, C1: 406, C2: 455
# Total number of combinations: 704*406*455 = 130 million combinations.
# This is too many and will result in memory errors!
# We can instead take the 30 top rated candidates from each list to get
# 27000 combinations.

# Generate the groups and calculate the average score
final = spark.createDataFrame(candidates[0].select("nconst", "score").head(30), ["nconst", "score"])
if len(candidates) > 1:
    for i, cand in enumerate(candidates[1:]):
        cand = spark.createDataFrame(cand.head(30), ["nconst", "score"])
        final = final.crossJoin(cand.selectExpr("nconst as nconst{}".format(i), "score as score{}".format(i)))

@udf("string")
def g_actors(row):
    actors = [row[i] for i in range(0, len(row), 2)]
    return ", ".join(actors)

@udf("float")
def g_score(row):
    scores = [float(row[i]) for i in range(1, len(row), 2)]
    return sum(scores) / len(scores)

columns = struct([final[x] for x in final.columns])
final = final.withColumn("actors", g_actors(columns)).withColumn("group_score", g_score(columns)).select("actors", "group_score")
final = final.orderBy(["group_score"], ascending=False)

final.write.csv("project/spark/result.tsv/", sep="\t", header=True)

# Disconnect from spark
spark.stop()






"""
This would be the better way to calcualte the similarity score because
work is done on every node, but it doesn't seem to work properly (execution time above 5 hours).
Probably because it has to load the tensorflow model for each partition (900MB) which is so
much of the memory that something has to be saved on disk instead of RAM.


def similarity_score():
    def executor(iterator):
        import sys
        if worker_module_path not in sys.path:
            sys.path.append(worker_module_path)
        
        import numpy as np
        import tensorflow_hub as hub

        sim_model = hub.load(sim_model_url)

        for i, row in enumerate(iterator):
            sim = sim_model([search_plot, row.summary])
            yield row.tconst, np.dot(sim[0], sim[1])

    return executor

partition_scores = spark.sql(movie_sql).rdd.mapPartitions(similarity_score())

sim_scores = partition_scores \
    .map(lambda row: (row[0], float(row[1]), )) \
    .toDF() \
    .select(col("_1").alias("tconst"), col("_2").alias("sim_score"))

movies = movies \
    .join(sim_scores, movies.tconst == sim_scores.tconst) \
    .drop(sim_scores.tconst) \
    .orderBy(["sim_score"], ascending=False)
"""