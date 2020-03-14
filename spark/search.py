import sys
import os
import ast

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf

search_genres = os.environ["search_genres"].split(",")
num_search_genres = len(search_genres)
search_actors = [[descr.split(" ")[0], *descr.split(" ")[1].split("-")] for descr in os.environ["search_actors"].split(",")]
for a in search_actors:
    if len(a) == 2:
        a.append(a[1])
search_plot = os.environ["search_plot"]

spark = SparkSession.builder.appName("PySpark Search").getOrCreate()

actors = spark.read.csv("project/spark/actors.tsv/part*", header=True, sep="\t")
actors = actors.withColumn("age", expr("2020 - birthYear"))

#movies = spark.read.csv("project/spark/movies.tsv/part*", header=True, sep="\t")
actors.createOrReplaceTempView("Actors")
#movies.createOrReplaceTempView("Movies")

@udf("float")
def calc_average_score(genre_score):
    score = 0
    genre_score = ast.literal_eval(genre_score)
    for genre in search_genres:
        score += genre_score.get(genre, -1000)
    return score / num_search_genres

sql = "SELECT nconst, genre_score FROM Actors WHERE age >= {0} AND age <= {1} AND gender= {2}"

candidates = []
for i, desc in enumerate(search_actors):
    cand = spark.sql(sql.format(desc[1], desc[2], 1 if desc[0] == "Female" else 0))

    genre_condition = cand.genre_score.contains(search_genres[0])
    if len(search_genres) > 1:
        for genre in search_genres[1:]:
            genre_condition &= cand.genre_score.contains(genre)

    cand = cand \
        .filter(genre_condition) \
        .select("nconst", calc_average_score("genre_score").alias("score")) \
        .orderBy(["score"], ascending=False)
    candidates.append(cand)

    cand.write.csv("project/spark/candidates{}.tsv/".format(i), sep="\t", header=True)
    
spark.stop()