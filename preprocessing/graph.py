#Generates a graph of relationships between actors
#Requires principals.tsv, ratings.tsv
#Outputs into graph.tsv


import pandas as pd 
import numpy as np 
import ast
import os
import re
import sys


#Collecting principal.tsv
principals=pd.read_csv('../data/principals.tsv', sep='\t')
 
#Collecting ratings.tsv
ratings=pd.read_csv('../data/ratings.tsv', sep='\t')

#Combining the dataframes and grouping them
df = pd.merge(principals, ratings, how='inner', left_on='tconst', right_on='tconst')
data=df.groupby(['tconst','averageRating'])['nconst'].apply(list).reset_index()

# Create dictionary with the edge value on the format
# edges[from_node][to_node] = value
def generate_edges(row):
    nconsts = row[2]
    rating = 10 - pd.to_numeric(row[1])
    edges = {}
    for i in range(len(nconsts)):
        edges[nconsts[i]] = {}
        for j in range(len(nconsts)):
            if i == j: continue
            edges[nconsts[i]][nconsts[j]] = rating
    return edges

#Creating edges 
edges=[None]*len(data)
for i in range(len(data)):
    edges[i]=(list(generate_edges(list(data.iloc[i])).items()))

#Appending edges to the dataframe
data['edges']=edges


# Create rows for each from_node [key value] where key is the 
# from_node and value is a dicitionary of edges.
e=data.explode('edges')['edges']
e=list(e)

# Convert to row = [key list] where key is the from_node and list
# has all of the edges from key to other nodes.
# list[index][to_node] = value
key=[]
value=[]
for i in range(len(e)):
    k,v=(e[i])
    key.append(k)
    value.append(v)

#Make a new dataframe with just key-vaue pairs
d=pd.DataFrame({'key': key,'value': value})

#Grouping by to find unique number of nodes(actors)
ed=d.groupby(['key'])['value'].apply(list).reset_index()

# Finds all edges from a node to the other nodes and only
# keeps the one with the lowest value.
def prune_edges(row):
    nconst = row[0]
    edges = {}
    all_edges = row[1]
    for edge_group in all_edges:
        for edge in edge_group:
            if edge_group[edge] < edges.get(edge, 11):
                edges[edge] = edge_group[edge]
    return str(edges)

#Creatibg pruned_edges
pruned_edges=[None]*len(ed)
for i in range(len(ed)):
    pruned_edges[i]=((prune_edges(list(ed.iloc[i]))))

#Appending pruned_edges to the dataframe
ed['values']=pruned_edges

# The graph is now complete and stored in the pruned dataframe on the format:
# Columns = [node, edges]. Node is the actor id.
# Edges is a dictionary on the format edges[to_actor] = value.
# Note: the edges column is a string representation of the dictionary
# so that it can be stored to a csv file.
pruned = ed.drop(['value'],axis=1)
pruned.to_csv("../data/graph.tsv", sep="\t", header=True)