# Evaluates the relationship between {}_actors.tsv
# Requires all {}_actor.tsv files, principals.tsv, input.txt
# Outputs into final.tsv
import pandas as pd 
import numpy as np 
import ast
import os
import re
import sys
import ast
import glob
import itertools

#Reading graph.tsv into graph dataframe
graph = pd.read_csv('../data/graph.tsv', sep='\t')
graph = graph.loc[:, ~graph.columns.str.contains('^Unnamed')]

# Converting values to dictionary and to list
graph['values'] = graph['values'].apply(lambda x: list(ast.literal_eval(x).items()))

# Exploding
new_graph=graph.explode('values')

# Multi-indexing 
new_graph[["to_node", "value"]]= pd.DataFrame(new_graph["values"].values.tolist(), index=new_graph.index)
new_graph.drop(columns=["values"], inplace=True)
new_graph.set_index(keys=["key", "to_node"], drop= True, inplace=True)

# Reading all {}_actor.tsv intoa list of dataframes
path = r'../data'
all_files = glob.glob(path + "/*_actor.tsv")
li = [pd.read_csv(filename, sep="\t", header=0) for filename in all_files]

# Collecting only the Actor_IDs
IDs = [list(df["Actor ID"]) for df in li]]

# Shortest path between a and b on new_graph
def relation_score(a,b,new_graph):
    step = {
        "node": a,
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
        if current["node"] == b:
            break
        # The distance to the next nodes is 1 more than the distance to this node
        new_dist = current["distance"] + 1
        # Values is a list of the graph values so that we can calculate the average when we are calculating the cost
        new_values = current["values"]
        # Find all of the edges from this node to it's neighbours as dict
        edges = new_graph.loc[current["node"]].to_dict()["value"]
        for edge in edges:
            if finished.get(edge): continue
            val = edges[edge]
            vals = new_values + [val]
            cost = 10 - (11 - new_dist + 10 - sum(vals) / new_dist) / 2

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

    actor = b
    path = []
    while actor != "":
        path.append(actor)
        actor = finished[actor]["prev"]
    path = list(reversed(path))
    score = 10 - finished[b]["cost"]

    return score


#All of the IDs from {}_actor.tsv with each other 
Id_list = list(itertools.product(*IDs))

#Id_list as a dataframe
data = pd.DataFrame(Id_list)

#Computing scoresfor all combinations in Id_list as average of all {}_actors
combs = []
for i in range(0, len(IDs)):
    for j in range(i+1, len(IDs)):
        combs.extend([(x, y, relation_score(x, y, new_graph)) for x in IDs[i] for y in IDs[j]])

#Making it to a dataframe
d1 = pd.DataFrame(combs, columns=["ID1", "ID2", "Score"])

#Initialising r_score to 0
data['r_score'] = 0.0

#Iteratively merging {}_actors to data and summing score
for i in range(len(li)):
    for j in range(i, len(li)):
        if (j + 1) < len(IDs):
            data = pd.merge(data,d1, how='inner', left_on=[i, j + 1], right_on=['ID1', 'ID2'])
            data['r_score'] = data['r_score'] + data['Score']
            data = data.drop(['ID1','ID2','Score'], axis=1)


#Finding average of r_score
data['r_score'] = data['r_score'] / len(li)

#Removing all zero scored values
data = data.loc[(data['r_score'] != 0)]
#Sorting 
data = data.sort_values(by='r_score', ascending=False)

#Merging r_score and gs_score
df = data
for i in range(len(li)):
    df = pd.merge(df, li[i], how='inner', left_on=i, right_on='Actor ID')
    sname = 'score_{}'.format(i)
    df[sname] = df['score']
    df = df.drop(['Actor ID', 'Avg Genre Score', 'summary_score', 'score'], axis=1)

#Finding mean of the gs_score of the {}_actors 
sname = []
for i in range(len(li)):
    sname.append('score_{}'.format(i))
col = df.loc[:, sname]
df['gs_score'] = col.mean(axis=1)

#Finding average of the r_score and gs_score and making the final list
df['final'] = df[["r_score","gs_score"]].mean(axis=1)
col_list = list(range(len(li))
col_list.append('final')
final_df = df[col_list]
final_df = final_df.sort_values(by='final', ascending=False)

#Outputting the final list as a tsv
final_df.to_csv('../data/final3_top25_latest.tsv', sep= '\t', header=True)