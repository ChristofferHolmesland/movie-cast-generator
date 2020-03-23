from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Connect to spark
spark = SparkSession.builder.appName("PySpark Graph").getOrCreate()

# Read data files
ratings = spark.read.csv("project/output/title_ratings/part*", header=True, sep="\t")
principals = spark.read.csv("project/output/title_principals/part*", header=True, sep="\t")

# Convert principals from [tconst nconst] to [tconst [nconst...]]
principals = principals \
            .groupby("tconst") \
            .agg(F.collect_list("nconst")) \
            .select("tconst", F.col("collect_list(nconst)").alias("nconst"))

# Add ratings to titles [tconst rating [nconst...]]
data = principals.join(ratings, principals.tconst == ratings.tconst).drop(ratings.tconst)

# Create dictionary with the edge value on the format
# edges[from_node][to_node] = value
@F.udf("map<string, map<string, float>>")
def generate_edges(row):
    nconsts = row[1]
    rating = 10 - float(row[2])
    edges = {}
    for i in range(len(nconsts)):
        edges[nconsts[i]] = {}
        for j in range(len(nconsts)):
            if i == j: continue
            edges[nconsts[i]][nconsts[j]] = rating
    return edges

columns = F.struct([data[x] for x in data.columns])
data = data.withColumn("edges", generate_edges(columns))

# Create rows for each from_node [key value] where key is the 
# from_node and value is a dicitionary of edges.
edges = data.select(F.explode(data.edges))
# Convert to row = [key list] where key is the from_node and list
# has all of the edges from key to other nodes.
# list[index][to_node] = value
edges = edges.groupby("key").agg(F.collect_list("value"))

# Finds all edges from a node to the other nodes and only
# keeps the one with the lowest value.
@F.udf("string")
def prune_edges(row):
    nconst = row[0]
    edges = {}
    all_edges = row[1]
    for edge_group in all_edges:
        for edge in edge_group:
            if edge_group[edge] < edges.get(edge, 11):
                edges[edge] = edge_group[edge]
    return str(edges)

columns = F.struct([edges[x] for x in edges.columns])
pruned = edges.select(F.col("key").alias("node"), prune_edges(columns).alias("edges"))   

# The graph is now complete and stored in the pruned dataframe on the format:
# Columns = [node, edges]. Node is the actor id.
# Edges is a dictionary on the format edges[to_actor] = value.
# Note: the edges column is a string representation of the dictionary
# so that it can be stored to a csv file.
pruned.write.csv("project/spark/graph.tsv/", sep="\t", header=True)

# Disconnect from spark
spark.stop()


# Using the graph
import ast
from pyspark.sql import functions as F

graph = spark.read.csv("project/spark/graph.tsv/part*", header=True, sep="\t")

@F.udf("map<string, float>")
def cast_string_to_map(col):
    return ast.literal_eval(col)

graph = graph.withColumn("edges", cast_string_to_map(graph.edges))

# Example where we find the cost to move between Leonardo DiCaprio
# and Anne Hathaway.
leonardo = "nm0000138"
anne = "nm0004266"

