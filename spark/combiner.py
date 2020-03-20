from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Connect to spark
spark = SparkSession.builder.appName("PySpark Combiner").getOrCreate()

# Load movie data
summaries = spark.read.csv("project/data/summary_box_office.tsv", header=True, sep="\t")
basics = spark.read.csv("project/output/title_basics/part*", header=True, sep="\t")
ratings = spark.read.csv("project/output/title_ratings/part*", header=True, sep="\t")
principals = spark.read.csv("project/output/title_principals/part*", header=True, sep="\t")

# Convert principals tconst -> nconst to nconst -> [tconst]
principals = principals \
            .groupby("nconst") \
            .agg(F.collect_list("tconst")) \
            .select("nconst", F.col("collect_list(tconst)").alias("tconst"))

principals = principals.withColumn("tconst", principals.tconst.cast("string"))

# Join movie data to one dataframe
movies = basics.join(summaries, basics.tconst == summaries.tconst).drop(summaries.tconst)
movies = movies.join(ratings, movies.tconst == ratings.tconst).drop(ratings.tconst)
movies = movies.filter(movies["summary"] != "N/A")

# Load actor data
actors = spark.read.csv("project/output/name/part*", header=True, sep="\t")
genre_scores = spark.read.csv("project/output/genre_scores/part*", header=True, sep="\t")

# Join actor data to one dataframe
actors = actors.join(genre_scores, actors.nconst == genre_scores.nconst).drop(genre_scores.nconst)
actors = actors.join(principals, actors.nconst == principals.nconst).drop(principals.nconst)
actors = actors.filter(actors["genre_score"] != "{}")

# Save dataframes to disk
movies.write.csv("project/spark/movies.tsv", sep="\t", header=True)
actors.write.csv("project/spark/actors.tsv", sep="\t", header=True)

# Disconnect from spark
spark.stop()