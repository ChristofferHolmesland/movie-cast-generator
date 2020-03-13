import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

search_genres = os.environ["search_genres"].split(",")
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

p_desc = search_actors[0]
sql = "SELECT nconst, genre_score FROM Actors WHERE age >= {0} AND age <= {1} AND gender= {2}"
primary_candidates = spark.sql(sql.format(p_desc[1], p_desc[2], 1 if p_desc[0] == "Female" else 0))

genre_condition = primary_candidates.genre_score.contains(search_genres[0])
if len(search_genres) > 1:
    for genre in search_genres[1:]:
        genre_condition &= primary_candidates.genre_score.contains(genre)

primary_candidates = primary_candidates.filter(genre_condition)

spark.stop()