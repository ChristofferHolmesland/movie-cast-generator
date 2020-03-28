# Dijkstra's algorithm on spark
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
# This is basically Dijkstra's shortest path algorithm
while len(queue) > 0:
    # Take the node with the lowest cost
    queue.sort(key=lambda x: x["cost"], reverse=True)
    current = queue.pop()
    # When you take a node from the queue it means that you have found the shortest path to that node.
    finished[current["node"]] = current
    print("Looking at " + current["node"] + ", distance: " + str(current["distance"]) + ", cost: " + str(current["cost"]))
    # Stop when we find christian
    if current["node"] == christian:
        print("Found path to Christian :D")
        break
    # The distance to the next nodes is 1 more than the distance to this node
    new_dist = current["distance"] + 1
    # Values is a list of the graph values so that we can calculate the average when we are calculating the cost
    new_values = current["values"]
    # Find all of the edges from this node to it's neighbours
    edges = graph.filter(graph.node == current["node"]).select("edges").collect()[0][0]
    for edge in edges:
        # If the edge is in finished it means that we already found a shorter path to that node
        if finished.get(edge): continue
        val = edges[edge]
        vals = new_values + [val]
        # Our scores (genre_score, similarity) are between 0 and 10. So we want this score to be in that range aswell.
        # The distance (11 - new_dist) means that shorter paths are preferred, A->B->C=9 instead of A->E->F->C=8.
        # (10 - sum(vals)/new_dist) is 10 - avg of the scores from the graph. The graph values are the opposite
        # of the rating, so a rating of 7 => 3, 9.5 => 0.5. This is done because we want to find the shortest path between actors.
        # If we didn't do this then the path would always try to visit every actor before finding christian. This is why we
        # subtract the average of the two numbers from 10. 
        cost = 10 - (11 - new_dist + 10 - sum(vals)/new_dist) / 2
        new_step = {
            "node": edge,
            "value": val,
            "distance": new_dist,
            "values": vals,
            "cost": cost,
            "prev": current["node"]
        }
        
        in_queue = False
        # Check if it is already in the queue, if it is and this cost is lower it should be updated
        for i in range(len(queue)):
            if queue[i]["node"] == edge:
                if queue[i]["cost"] > new_step["cost"]:
                    queue[i] = new_step
                in_queue = True
                break
        if in_queue: continue
        # If the edge was not in the queue it is added.
        queue.append(new_step)

actor = christian
path = []
while actor != "":
    path.append(actor)
    actor = finished[actor]["prev"]
path = list(reversed(path))
print("Leonardo-Christian score: " + str(10 - finished[christian]["cost"]))



# First iteration of the bfs algorithm
current = graph.filter(graph.node == leonardo)
next = current.select(F.explode(current.edges))
next = next.withColumn("distance", F.lit(1))
next = next.join(graph, next.key == graph.node).drop(graph.node)
next2 = next.select("key", "value", F.col("distance") + 1, F.explode(next.edges))


import time
def bfs():
    start_time = time.time()
    current = graph.filter(graph.node == leonardo)
    next = current.select(F.col("node").alias("start"), F.explode(current.edges).alias("step1", "value1"))
    next = next.join(graph, next.step1 == graph.node).drop(graph.node)
    next2 = next.select("*", F.explode(next.edges).alias("step2", "value2")).drop(next.edges)
    next2 = next2.join(graph, next2.step2 == graph.node).drop(graph.node)
    next3 = next2.select("*", F.explode(next2.edges).alias("step3", "value3")).drop(next2.edges)
    next3 = next3.join(graph, next3.step3 == graph.node).drop(graph.node)
    next4 = next3.select("*", F.explode(next3.edges).alias("step4", "value4")).drop(next3.edges)
    #next4 = next4.join(graph, next4.step4 == graph.node).drop(graph.node)
    res = next4.count()
    end_time = time.time()
    print("Time spent: " + str(end_time - start_time))
    return res

next5 = next4.select("*", F.explode(next4.edges).alias("step5", "value5")).drop(next4.edges)
next5 = next5.join(graph, next5.step5 == graph.node).drop(graph.node)

cond = (next4.step1 == christian) | (next4.step2 == christian) | (next4.step3 == christian) | (next4.step4 == christian)



n = 1
next.count() == 350
cond = (next.step1 == christian)
next.filter(cond).count() == 1
# Time 12.37 seconds

n = 2
next2.count() == 58 339
cond = (next4.step1 == christian) | (next4.step2 == christian)
next2.filter(cond).count() == 372
# Time 13.94 seconds

n = 3
next3.count() == 8 901 575
cond = (next4.step1 == christian) | (next4.step2 == christian) | (next4.step3 == christian)
next3.filter(cond).count() == 77 087
# Time 179.08 seconds

n = 4
next4.count() == 1 354 994 561
cond = (next4.step1 == christian) | (next4.step2 == christian) | (next4.step3 == christian) | (next4.step4 == christian)
next4.filter(cond).count() == 12 798 876


import time
import pyspark.sql.types as _type

# Breadth first search from start to goal on graph.
# n is the maximum number of steps to take.
def bfs(start, goal, graph, n):
    # Calculates the cost from start to the goal using the path
    # in the row, if a path exists.
    def __calc_score(row):
        # columns 1,3,5... are actor ids
        # columns 2,4,6... are the values
        for i in range(1, len(row), 2):
            if row[i] == goal:
                vals = [row[j] for j in range(i+1, 1, -2)]
                dist = len(vals)
                return (11 - dist + 10 - sum(vals)/dist) / 2
        return -1
    _calc_score = F.udf(__calc_score, "float")
    # Start with a dataframe containing only the start node
    current = graph.filter(graph.node == start)
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
    # Find and return the maximum
    max_score = current.agg({"score": "max"}).collect()[0][0]
    return max_score

s = time.time()
res = bfs(leonardo, christian, graph, 2)
s2 = time.time()
print(s2 - s)
print(res)

# Running time example when the input is (start_node, end_node, graph, n)
# Assume we are looking for three actors and we only 
# make groups from the top 20 actors from each list.
# Number of bfs calls: 
#  A  B      A  C      B   C
# (20*20) + (20*20) + (20*20) = 1200
# If we assume bfs() executes in 1 second, then
# 1200 / 60 = 20 minutes to calculate 1200 relations.
# 

# If we observe that calculating A1-B1, A2-B1, ..., An-B1
# are the same n steps from start with a different condition 
# in the __calc_score function, then we can try to optimize by
# only calculating the n steps from start once.

ben = "nm0000255"
goals = [christian, ben]

def bfs_grouped(start, goals, graph, n):
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
    # Start with a dataframe containing only the start node
    current = graph.filter(graph.node == start)
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
    # Split by goal and find maximums
    current = current.select(F.explode(F.col("score")).alias("goal", "score"))
    maximums = current.groupby(["goal"]).max("score").collect()
    result = {}
    for row in maximums:
        result[row[0]] = row[1]
    return result


s = time.time()
res = bfs_grouped(leonardo, goals, graph, 2)
s2 = time.time()
print(s2 - s)
# Execution time: 17.72 seconds
# time/relation = 8.86 seconds

s = time.time()
res = bfs(leonardo, christian, graph, 2)
res = bfs(leonardo, ben, graph, 2)
s2 = time.time()
print(s2 - s)
# Execution time: 27.07 seconds
# time/relation = 13.54 seconds

# A more realistic example with more actors
goals = [christian, ben, "nm1297015", "nm0331516", "nm0799777", "nm0001497", "nm0000379", "nm0290556"]
s = time.time()
res = bfs_grouped(leonardo, goals, graph, 2)
s2 = time.time()
print(s2 - s)
# Execution time 18.29 seconds
# time/relation 2.29 seconds

s = time.time()
res = [bfs(leonardo, actor, graph, 2) for actor in goals]
s2 = time.time()
print(s2 - s)
# Execution time 166.04 seconds
# time/relation 20.76 seconds