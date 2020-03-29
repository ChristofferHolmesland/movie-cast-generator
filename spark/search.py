import sys
import os
import ast
import pwd

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

graph = spark.read.csv("project/spark/graph.tsv/part*", header=True, sep="\t")

# Read actors froms file to DataFrame, and calculate their age.
actors = spark.read.csv("project/spark/actors.tsv/part*", header=True, sep="\t")
actors = actors.withColumn("age", F.expr("2020 - birthYear"))

# Read movies from file to DataFrame.
movies = spark.read.csv("project/spark/movies.tsv/part*", header=True, sep="\t")

# Create views to enable SQL statements.
actors.createOrReplaceTempView("Actors")
movies.createOrReplaceTempView("Movies")

movie_sql = "SELECT tconst, summary from Movies WHERE summary != 'N/A'"
candidate_sql = "SELECT nconst, genre_score, tconst FROM Actors WHERE age >= {0} AND age <= {1} AND gender= {2}"

# Remove movies without a summary
movies = spark.sql(movie_sql)

@F.udf("map<string, float>")
def cast_string_to_map(col):
    return ast.literal_eval(col)

graph = graph.withColumn("edges", cast_string_to_map(graph.edges))

# Decorator indicating that calc_average_score is an user-defined-function returning a float value.
@F.udf("float")
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

@F.udf("array<string>")
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
    cand = cand.join(movies, F.array_contains(cand.tconst, movies.tconst)).drop(movies.tconst)
    d = cand.groupby("nconst").agg({"sim_score": "max"})
    cand = cand.join(d, cand.nconst == d.nconst).drop(d.nconst)
    cand = cand.select("nconst", calc_average_score("genre_score").alias("avg_genre_score"), F.col("max(sim_score)").alias("max_sim_score"))
    cand = cand.withColumn("score", (cand.avg_genre_score + cand.max_sim_score) / 2)
    cand = cand.drop_duplicates(["nconst"]).orderBy(["score"], ascending=False)
    candidates.append(cand)

    # Save candidates to disk
    cand.write.csv("project/spark/candidates{}.tsv/".format(i), sep="\t", header=True)

# Number of actors fitting the Intersteller search
# C0: 704, C1: 406, C2: 455
# Total number of combinations: 704*406*455 = 130 million combinations.
# This is too many and will result in memory errors!
# We can instead take the 30 top rated candidates from each list to get
# 27000 combinations.

def bfs_multigrouped(starts, goals, graph, n):
    # Calculates the cost from start to the goals using the path
    # in the row, if a path exists.
    def __calc_score(row):
        # columns 1,3,5... are actor ids
        # columns 2,4,6... are the values
        goal = {g: -1 for g in goals}
        for i in range(1, len(row), 2):
            if row[i] in goal and goal[row[i]] == -1:
                vals = [row[j] for j in range(i+1, 1, -2)]
                dist = len(vals)
                goal[row[i]] = (11 - dist + 10 - sum(vals)/dist) / 2
        return goal
    _calc_score = F.udf(__calc_score, "map<string,float>")
    # Start with a dataframe containing only the start nodes
    current = graph.filter(F.col("node").isin(starts))
    # Create rows for each edge from the start node
    current = current.select(F.col("node").alias("start"), F.explode(current.edges).alias("step1", "value1"))
    # Add the edges of the edges to the dataframe
    current = current.join(graph, current.step1 == graph.node).drop(graph.node)
    # For each breadth step
    for i in range(1, n):
        # Create rows from the edges of each row
        current = current.select("*", F.explode(current.edges).alias("step{}".format(i+1), "value{}".format(i+1))).drop(current.edges)
        # If this is not the last step then add more edges
        if i == n-1: break
        current = current.join(graph, current["step{}".format(i+1)] == graph.node).drop(graph.node)
    # Calculate the score of each row (path)
    columns = F.struct([current[x] for x in current.columns])
    current = current.withColumn("score", _calc_score(columns))
    # Split by start,goal and find maximums
    current = current.select("start", F.explode(F.col("score")).alias("goal", "score"))
    maximums = current.groupby(["start", "goal"]).max("score").collect()
    result = {}
    for row in maximums:
        # Row[0] = start, Row[1] = goal, Row[2] = score
        if row[0] not in result:
            result[row[0]] = {}
        result[row[0]][row[1]] = row[2]
    return result

# Generate the groups and calculate the average score
first_actors = candidates[0].select("nconst", "score").head(30)
ids = [[a[0] for a in first_actors]]
final = spark.createDataFrame(first_actors, ["nconst", "score"])
if len(candidates) > 1:
    for i, cand in enumerate(candidates[1:]):
        actors = cand.select("nconst", "score").head(30)
        ids.append([a[0] for a in actors])
        cand = spark.createDataFrame(actors, ["nconst", "score"])
        final = final.crossJoin(cand.selectExpr("nconst as nconst{}".format(i), "score as score{}".format(i)))

search_groups = []
for i in range(len(ids) - 1):
    search_groups.append([ids[i], []])
    for j in range(i + 1, len(ids)):
        search_groups[i][1].extend(ids[j])

graph.cache()
relations = {}
for group in search_groups:
    res = bfs_multigrouped(group[0], group[1], graph, 2)
    for node in res:
        if node not in relations:
            relations[node] = {}
        for to_node in res[node]:
            relations[node][to_node] = res[node][to_node]

@F.udf("string")
def g_actors(row):
    actors = [row[i] for i in range(0, len(row), 2)]
    return ", ".join(actors)

@F.udf("float")
def g_score(row):
    scores = [float(row[i]) for i in range(1, len(row), 2)]
    return sum(scores) / len(scores)

@F.udf("float")
def calc_final_score(row):
    actors = row[0].split(", ")
    relation_scores = []
    for i in range(len(actors)):
        for j in range(i + 1, len(actors)):
            relation_scores.append(relations[actors[i]][actors[j]])
    not_none = [score for score in relation_scores if score != None]
    n_not_none = len(not_none)
    if n_not_none == 0:
        return row[1]
    relation_ratio = (1 / 3) * n_not_none / len(relation_scores)
    return (1 - relation_ratio) * row[1] + relation_ratio * sum(not_none) / n_not_none

columns = F.struct([final[x] for x in final.columns])
final = final.withColumn("actors", g_actors(columns)).withColumn("group_score", g_score(columns)).select("actors", "group_score")

new_columns = F.struct([final[x] for x in final.columns])
final = final.withColumn("final_score", calc_final_score(new_columns))

final = final.orderBy(["final_score"], ascending=False)
final.write.csv("project/spark/result.tsv/", sep="\t", header=True)

# Disconnect from spark
spark.stop()


"""
This would be the better way to calcualte the similarity score because
work is done on every node, but it doesn't seem to work properly (execution time above 5 hours).
Probably because it has to load the tensorflow model for each partition (900MB) which is so
much of the memory that something has to be saved on disk instead of RAM.


worker_module_path = "/home/ubuntu/.local/lib/python3.5/site-packages/"

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
    .select(F.col("_1").alias("tconst"), F.col("_2").alias("sim_score"))

movies = movies \
    .join(sim_scores, movies.tconst == sim_scores.tconst) \
    .drop(sim_scores.tconst) \
    .orderBy(["sim_score"], ascending=False)
"""