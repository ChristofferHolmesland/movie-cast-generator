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
# and Christian Bale
leonardo = "nm0000138"
christian = "nm0000288"

#step = {
#   "node": "",
#   "value": "",
#   "distance": 0,
#   "values" = [],
#   "cost" = (11 - distance) + 10 - (sum(values)/len(values)),
#   "prev" = step_prev
#}

# This is the starting step
step = {
    "node": leonardo,
    "value": 0,
    "distance": 0,
    "values": [],
    "cost": 0,
    "prev": ""
}

# This is all of the nodes we already found the shortest path to
finished = {}
# This is the next steps to consider
queue = [step]
while len(queue) > 0:
    queue.sort(key=lambda x: x["cost"], reverse=True)
    current = queue.pop()
    finished[current["node"]] = current
    print("Looking at " + current["node"] + ", distance: " + str(current["distance"]) + ", cost: " + str(current["cost"]))
    if current["node"] == christian:
        print("Found path to Christian :D")
        break
    new_dist = current["distance"] + 1
    new_values = current["values"]
    edges = graph.filter(graph.node == current["node"]).select("edges").collect()[0][0]
    for edge in edges:
        if finished.get(edge): continue
        val = edges[edge]
        vals = new_values + [val]
        cost = 10 - (11 - new_dist + 10 - sum(vals)/new_dist) / 2
        # check if it is already in the queue, if it is and this cost is lower it should be updated
        new_step = {
            "node": edge,
            "value": val,
            "distance": new_dist,
            "values": vals,
            "cost": cost,
            "prev": current["node"]
        }
        
        in_queue = False
        for i in range(len(queue)):
            if queue[i]["node"] == edge:
                if queue[i]["cost"] > new_step["cost"]:
                    queue[i] = new_step
                in_queue = True
                break
        if in_queue: continue
        queue.append(new_step)

actor = christian
path = []
while actor != "":
    path.append(actor)
    actor = finished[actor]["prev"]
path = list(reversed(path))
print("Leonardo-Christian score: " + str(10 - finished[christian]["cost"]))